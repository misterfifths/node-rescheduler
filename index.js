/* jshint -W097 */
'use strict';

var events = require('events'),
    util = require('util'),
    _ = require('underscore'),
    luaScript;


// This is the main piece of logic, executed as a server-side script via Redis' Lua interface.
// We store payloads in a sorted set, with their score set to the timestamp of when they're
// supposed to be enqueued for processing. Then, every so often, we run this script, which
// checks for any newly ready payloads (zrangebyscore with the current time as the maximum
// score), pushes them onto the destination queue (rpush, which maintains the sort order
// from the sorted set), and then removes them from the set (zremrangebyscore).
// Since scripts are run atomically by Redis, we don't need to do a piecemeal zrem for
// each of the payloads; the set cannot have changed between the zrangebyscore and
// zremrangebyscore calls.
// So, as a script, this takes 2 keys (the zset and the destination queue), and
// one argument, the maximum timestamp to enqueue.
luaScript = [ 'local winners = redis.call("zrangebyscore", KEYS[1], 0, ARGV[1])',
              'if #winners == 0 then return 0 end',

              'redis.call("rpush", KEYS[2], unpack(winners))',
              'redis.call("zremrangebyscore", KEYS[1], 0, ARGV[1])',
              'return #winners'
            ].join('\n');


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
            var intervalId = setInterval(function() {
                self.checkNow(function(err) {
                    if(err) self.emit('error', err);
                });
            }, self.options.checkInterval);

            defineReadonlyProperty('stopChecking', function() {
                if(intervalId) clearInterval(intervalId);
                intervalId = undefined;
            }, false);
        })();
    }
}

module.exports = ReScheduler;

util.inherits(ReScheduler, events.EventEmitter);

ReScheduler.prototype = Object.create(ReScheduler.prototype, {
    enqueueAt: {
        value: function(date, payload, callback) {
            var timestamp = (typeof date === 'number') ? date : date.getTime();
            this.client.zadd([ this.zsetName, timestamp, payload ], callback);
        },
        writable: true
    },

    enqueueIn: {
        value: function(minutes, payload, callback) {
            this.enqueueAt((new Date()).getTime() + minutes * 60 * 1000, payload, callback);
        },
        writable: true
    },

    scheduledCount: {
        value: function(callback) {
            this.client.zcard(this.zsetName, callback);
        },
        writable: true
    },

    checkNow: {
        value: function(callback) {
            var self = this,
                maxScore = (new Date()).getTime();

            this.client.eval([ luaScript, 2, this.zsetName, this.targetQueue, maxScore ], callback);
        },
        writable: true
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
        },
        writable: true
    },

    shutdown: {
        value: function(leaveClientOpen) {
            if(this.stopChecking) this.stopChecking();
            if(!!leaveClientOpen) this.client.quit();
        }
    },

    qWrap: {
        value: (function() {
            var wrapped = false;

            return function(Q) {
                if(!wrapped) {
                    var cbFns = [ 'enqueueAt', 'enqueueIn', 'scheduledCount', 'checkNow', 'pop' ],
                        i;

                    if(!Q) Q = require('q');  // This will throw if q isn't installed, which is fine.

                    for(i = 0; i < cbFns.length; i++)
                        wrapFunction(this, cbFns[i], Q);

                    wrapped = true;
                }

                return this;
            };
        })()
    }
});

function wrapFunction(rs, fnName, Q) {
    var originalFn = rs[fnName],
        originalArity = originalFn.length;

    rs[fnName] = function() {
        if(arguments.length === originalArity)
            return originalFn.apply(rs, arguments);

        return Q.npost(rs, fnName, arguments);
    };
}