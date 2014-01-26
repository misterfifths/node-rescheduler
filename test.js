require('nodefly-v8-profiler');

var RQ = require('.'),
    redis = require('redis'),
    shedulingClient = redis.createClient(),
    popClient = redis.createClient(),
    Q = require('Q');


var queueName = 'myqueue',
    rq = new RQ(shedulingClient, queueName, { checkInterval: 1000 }).qWrapper;

/*rq.enqueueIn(0.25, 'payload 1', redis.print);
rq.enqueueIn(0.5, 'payload 2', redis.print);

function loop() {
    rq.pop(popClient, function(err, res) {
        if(err) throw err;

        console.log('unqueued!', res);
        loop();
    });
}

loop();*/

function loop() {
    return rq.pop(popClient)
        .then(function(payload) {
            console.log('unqueued', payload);

            return loop();
        })
        .fail(function(err) {
            throw err;
        });
}

setInterval(function() {}, 1 * 1000);

rq.shutdown();
delete rq;

/*Q.all([rq.enqueueIn(0.25, 'payload 1'), rq.enqueueIn(0.5, 'payload 2')])
    .then(function(results) {
        console.log('Enqueue results:', results);

        return loop();
    })
    .done();*/
