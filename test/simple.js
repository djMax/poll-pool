var async = require('async'),
    assert = require('assert'),
    PollPool = require('../');

require('longjohn');

describe('poll-pool', function () {

    var poller1, poller2;

    before(function (setupDone) {
        async.parallel([function (done) {
            poller1 = new PollPool({
                agentName: 'poller1',
                url: 'mongodb://localhost:27017/poll-pool-test-db'
            }, done);
        }, function (done) {
            poller2 = new PollPool({
                agentName: 'poller2',
                url: 'mongodb://localhost:27017/poll-pool-test-db'
            }, done);
        }], setupDone);
    });

    it('should start a polling job on poller 1', function (done) {
        poller1.startPolling({
            key: 'sharedjob',
            durationSeconds: 10,
            claimedCallback: function (e) {
                // let mongo cursor tell poller2 about the claim. In the real world this race condition would just result in two pollers
                setTimeout(done, 250);
            },
            progressCallback: function (e,i) {
                // console.log('poller1 got progress.',i);
            },
            poller: function firstPoller(options, runIndex, doneFunction) {
                console.log('poller1 running', runIndex);
                if (runIndex < 5) {
                    return doneFunction(null, 100); // wait 100 msec and run again.
                } else if (runIndex < 10) {
                    return setTimeout(function () {
                        doneFunction(null, 100); // wait 100 msec but don't say so until 50 msec
                    }, 50);
                }
                setTimeout(function () {
                    doneFunction(null, {ok: true});
                }, 100); // success in 100 msec
            }
        }, function (error, result) {
            console.log('poller1 finished.', result);
        });
    });

    it('should wait for the polling job to finish', function (done) {
        poller2.startPolling({
            key: 'sharedjob',
            durationSeconds: 10,
            poller: function secondPoller() {
                console.log('poller2 running!');
                assert(false);
            },
            progressCallback: function (e,i) {
                // console.log('poller2 got progress.',i);
            },
            claimedCallback: function (e) {
                // We shouldn't claim a job that already has a runner
                assert(false);
            }
        }, function (e,r) {
            console.log('poller2 finished.', r);
            assert(r.result.ok === true);
            done();
        });
    });
});