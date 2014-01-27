var RQ = require('.'),
    redis = require('redis'),
    shedulingClient = redis.createClient(),
    popClient = redis.createClient(),
    Q = require('Q');


var queueName = 'myqueue',
    rq = new RQ(shedulingClient, queueName, { checkInterval: 1000, forceNoServerEval: true }).qWrap();

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

var soon = (new Date()).getTime() + 1000;

Q.all([rq.enqueueAt(soon, 'payload 1'), rq.enqueueAt(soon, 'payload 2'), rq.enqueueAt(soon, 'payload 3'), rq.enqueueIn(0.5, 'payload 4')])
    .then(function(results) {
        console.log('Enqueue results:', results);

        return loop();
    })
    .done();
