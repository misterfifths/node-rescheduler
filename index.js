/* jshint -W097 */
'use strict';

var _ = require('underscore'),
    qVersionRequirement = '~0.9.6',
    Q;

function ReScheduler(redisClient, targetQueue, options) {
    var self = this;
    function defineReadonlyProperty(name, value, enumerable) {
        Object.defineProperty(self, name, { value: value, enumerable: enumerable !== false });
    }

    defineReadonlyProperty('client', redisClient);
    defineReadonlyProperty('targetQueue', targetQueue);
    defineReadonlyProperty('zsetName', targetQueue + '-scheduler');

    options = options ? _.clone(options) : {};
    _.defaults(options, {
        checkInterval: 60 * 1000
    });
    defineReadonlyProperty('options', Object.freeze(options));

    if(this.options.checkInterval > 0) {
        (function() {
            var intervalId = setInterval(self.checkNow.bind(self), self.options.checkInterval);

            defineReadonlyProperty('stopChecking', function() {
                if(intervalId) clearInterval(intervalId);
                intervalId = undefined;
            }, false);
        })();
    }

    if(Q)
        defineReadonlyProperty('qWrapper', qWrap(this), false);
    else {
        Object.defineProperty(self, 'qWrapper', { get: function() {
            throw new Error('The qWrapper functionality requires Q satisfying version ' + qVersionRequirement);
        }}, false);
    }
}

module.exports = ReScheduler;

ReScheduler.prototype = Object.create(ReScheduler.prototype, {
    enqueueAt: {
        value: function(date, payload, callback) {
            var timestamp = (typeof date === 'number') ? date : date.getTime();
            this.client.zadd([ this.zsetName, timestamp, payload ], callback);
        }
    },

    enqueueIn: {
        value: function(minutes, payload, callback) {
            this.enqueueAt((new Date()).getTime() + minutes * 60 * 1000, payload, callback);
        }
    },

    scheduledCount: {
        value: function(callback) {
            this.client.zcard(this.zsetName, callback);
        }
    },

    checkNow: {
        value: function(callback) {
            var self = this,
                maxScore = (new Date()).getTime();

            // Pull everything out of the zset with scores up to the current time
            this.client.zrangebyscore([ self.zsetName, 0, maxScore ], function(err, scheduledPayloads) {
                if(err) {
                    if(callback) callback(err);
                    return;
                }

                if(scheduledPayloads.length === 0) {
                    if(callback) callback(null, 0);
                    return;
                }

                // Modify the payloads array so it's suitable as arguments for rpush, and push those commands onto the target queue.
                scheduledPayloads.unshift(self.targetQueue);
                self.client.rpush(scheduledPayloads, function(err) {
                    if(err) {
                        if(callback) callback(err);
                        return;
                    }

                    // And finally, remove the values we just processed from the set. Rejiggering the payloads array for zrem.
                    scheduledPayloads.shift();
                    scheduledPayloads.unshift(self.zsetName);
                    self.client.zrem(scheduledPayloads, function(err) {
                        if(callback) {
                            if(err) callback(err);
                            else callback(null, scheduledPayloads.length - 1);
                        }
                    });
                });
            });
        }
    },

    pop: {
        value: function(client, timeout, callback) {
            if(typeof timeout == 'function') {
                callback = timeout;
                timeout = 0;
            }

            client.blpop([ this.targetQueue, timeout ], function(err, res) {
                if(callback) {
                    if(err) callback(err);
                    else callback(null, res[1]);
                }
            });
        }
    },

    shutdown: {
        value: function(leaveClientOpen) {
            if(this.stopChecking) this.stopChecking();
            if(!!leaveClientOpen) this.client.quit();
        }
    }
});

function qWrap(rs) {
    var cbFns = [ 'enqueueAt', 'enqueueIn', 'scheduledCount', 'checkNow', 'pop' ],
        passthroughFns = [ 'stopChecking', 'shutdown' ],
        wrapper = {},
        props = Object.keys(rs),
        i, fn, prop;

    for(i = 0; i < cbFns.length; i++) {
        fn = cbFns[i];
        Object.defineProperty(wrapper, fn, {
            value: Q.nbind(rs[fn], rs)
        });
    }

    for(i = 0; i < passthroughFns.length; i++) {
        fn = passthroughFns[i];
        if(rs[fn]) {
            Object.defineProperty(wrapper, fn, {
                value: rs[fn].bind(rs)
            });
        }
    }

    for(i = 0; i < props.length; i++) {
        prop = props[i];
        Object.defineProperty(wrapper, prop, Object.getOwnPropertyDescriptor(rs, prop));
    }

    return wrapper;
}

(function() {
    var semver = require('semver'),
        qPackageInfo;

    try {
        Q = require('q');
        qPackageInfo = require('q/package.json');

        if(!semver.satisfies(qPackageInfo.version, qVersionRequirement))
            Q = undefined;
    }
    catch(e) {
        if(e.code === 'MODULE_NOT_FOUND')
            return;

        throw e;
    }
})();