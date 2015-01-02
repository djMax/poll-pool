var mubsub = require('mubsub'),
    os = require('os');

/**
 * A PollPool will use a centralized MongoDB collection to coordinate periodically polling a
 * service across a set of servers while generally minimizing duplicate work. It's not perfect,
 * as "active workers" are received as they occur and thus at process startup, existing workers
 * will be missed.
 */
var PollPool = function (config) {

    function getClient() {
        if (config.url) {
            return mubsub(config.url);
        }

        if (config.client) {
            return mubsub(config.client);
        }
        throw new Error('client or url must be specified in the poll-pool configuration argument');
    }

    var self = this;
    var client = getClient();
    this.collection = config.collection || 'poll-pool';
    this.channel = client.channel(this.collection, config.mubsub);
    this.agentName = config.agentName || (os.hostname() + process.pid);
    this.jobs = {};

    this.channel.on('message', function (message) {
        console.log('Message received.', message);
    });

    this.channel.on('claimKey', function (message) {
        console.log('Poll key claimed.', message);
    });

    this.channel.on('ready', function () {
        self.subscribed = true;
        console.log('poll-pool agent ' + self.agentName + ' connected to MongoDB using collection ' + self.collection || 'poll-pool');
    });

};

/**
 *
 * @param options
 *  key: the unique identifier for the job which is used to find other workers doing the same job.
 *  durationSeconds: the number of seconds this polling should continue
 *  poller: the function to be executed to run the job. The function takes (options, runIndex, callback) and should
 *      invoke the callback when it completes the poll attempt (standard error/result callback). The result of the callback
 *      is ANY OF:
 *          An error - stop polling, notify all listeners of an error.
 *          An object - notify all listeners with the result property and stop polling locally if no 'next' property is present.
 *             If the 'next' property is present, we'll run again in 'next' milliseconds. As a convenience, you can also just
 *             return a number to simulate returning {next:n}
 *  callback: A function to be called when polling completes (either via timeout or actual completion)
 */
PollPool.prototype.startPolling = function (options, callback) {
    // TODO what if we stop polling while someone else would like to continue... Need to "pick up" the polling
    // and claim it so that not everybody claims when poller A finishes
    if (!options.key) {
        throw new Error('startPolling requires the options contain a \'key\' property to identify this job for potential sharing.');
    }
    if (!options.poller) {
        throw new Error('startPolling requires a polling function.');
    }
    var info = this.jobs[options.key];
    if (!info) {
        info = this.jobs[options.key] = {
            locals: [],
            key: options.key,
            pollers: [this.agentName]
        };
        this.channel.publish('claimKey', {key:options.key});
    }
    var pollPool = this;
    info.locals.push({options:options,callback:callback});
    if (info.pollers[0] === this.agentName) {
        // Let's go let's go.
        var runIndex = 1;
        var pollExecutor = function () {
            options.poller(options, runIndex++, function pollerResultHandler(pollError, pollResult) {
                if (typeof(pollResult) === 'number') {
                    pollResult = {next:pollResult};
                }
                console.log(pollResult);
                pollPool.channel.publish('ran', {key: options.key, result: pollResult});
                if (pollResult.next) {
                    setTimeout(pollExecutor, pollResult.next);
                } else {
                    pollPool.channel.publish('done', {key: options.key});
                }
            });
        };
        process.nextTick(pollExecutor);
    }
};

module.exports = PollPool;