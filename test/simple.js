var PollPool = require('../');

require('longjohn');

describe('poll-pool', function () {

    var poller1, poller2;

    before(function () {
        poller1 = new PollPool({
            url: 'mongodb://localhost:27017/poll-pool-test-db'
        });
        /*
        poller2 = new PollPool({
            url: 'mongodb://localhost:27017/poll-pool-test-db'
        });*/
    });

    it('should start a polling job on poller 1', function () {
       poller1.startPolling({
          key: 'sharedjob',
           durationSeconds: 10,
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
                   doneFunction(null, {ok:true});
               }, 100); // success in 100 msec
           }
       }, function (error, result) {
           console.log('poller1 finished.');
       });
    });

    it('should wait for the polling job to finish', function (done) {

    });
});