'use strict';

var events = require('events'),
    util = require('util'),
    luaScript,
    minVersionForEval = [ 2, 6, 0 ];


// For Redis 2.6.0+, we get some goodness:
// This is the main piece of logic, executed as a server-side script via Redis' Lua interface.
// We store payloads in a sorted set, with their score set to the timestamp of when they're
// supposed to be enqueued for processing. Then, every so often, we run this script, which
// checks for any newly ready payloads (zrangebyscore with the current time as the maximum
// score), pushes them onto the destination list (rpush, which maintains the sort order
// from the sorted set), and then removes them from the set (zremrangebyscore).
// Since scripts are run atomically by Redis, we don't need to do a piecemeal zrem for
// each of the payloads; the set cannot have changed between the zrangebyscore and
// zremrangebyscore calls.
// So, as a script, this takes 2 keys (the zset and the destination list), and
// one argument, the maximum timestamp to enqueue.
// Doing this via the Lua interface saves us having to needlessly pull down all the values
// we're dealing with, only to push them back up to Redis and discard them -- this way
// the only value returned to us is an integer. Yay!
luaScript = [ 'local winners = redis.call("zrangebyscore", KEYS[1], 0, ARGV[1])',  // note that arrays are 1-based in Lua
              'if #winners == 0 then return 0 end',  // the # operator gives the length of an array

              'redis.call("rpush", KEYS[2], unpack(winners))',  // unpack turns an array into 1-by-1 arguments (akin to function.apply() in JS)
              'redis.call("zremrangebyscore", KEYS[1], 0, ARGV[1])',
              'return #winners'
            ].join('\n');

//  ... for <2.6.0, we have to pull down the zrangebyscore results, then do the rpush, then do a
// piecemeal zrem. Ugh.



// An object that handles enqueuing values for future processing in Redis.
//
// redisClient (created via redis.createClient()) must be dedicated solely to this instance; no other
// operations should be performed on it until this object is shut down.
//
// targetList is the name of the Redis list onto which values will be pushed when it is time for
// them to be executed. They will be pushed onto this list using the 'rpush' command. Consumption
// of values whose time has come is intended to happen via a simple 'lpop' or 'blpop' on the
// given list. The pop() method of this class is just a wrapper for 'blpop'; another module
// could easily consume values without any knowledge of ReScheduler.
//
// options is an optional hash, with two configuration properties:
// 'checkInterval' is the number of milliseconds between checks for values whose execution time
// has arrived. It defaults to one minute. Provide a value of 0 to disable automatic checking;
// if you do this, it is up to you to intermittently call checkNow() to process values.
//
// 'forceNoServerEval', if true, will disable use of Redis' server-side Lua scripting, even
// if the server supports it (2.6.0+). This is not recommended, as the Lua interface makes
// the scheduling operation quicker, atomic, and less bandwidth-intensive.
//
// Note that the accuracy of when a value is enqueued is entirely dependent on how often
// checkNow() is called, either via checkInterval or manually. If you need high-resolution
// (e.g. less than a second), this is not the class for you.
//
// If automatic checking is enabled, you may call stopChecking() to cancel it.
// If an error occurs while the automatic checking is running, the 'error' event will be emitted
// with the related Error object.
//
// Like promises? Call qWrap() and all callback functions will return promises instead.
function ReScheduler(redisClient, targetList, options) {
    var self = this,
        _options, key;

    function defineReadonlyProperty(name, value, enumerable) {
        Object.defineProperty(self, name, { value: value, enumerable: enumerable !== false });
    }

    defineReadonlyProperty('client', redisClient);
    defineReadonlyProperty('targetList', targetList);
    defineReadonlyProperty('zsetName', targetList + '-scheduler');

    _options = { checkInterval: 60 * 1000, forceNoServerEval: false };
    for(key in _options) {
        if(options.hasOwnProperty(key) && options[key] !== undefined)
            _options[key] = options[key];
    }

    defineReadonlyProperty('options', Object.freeze(_options));

    if(this.client.ready) onReady.call(this);
    else this.client.once('ready', onReady.bind(this));
}

module.exports = ReScheduler;

util.inherits(ReScheduler, events.EventEmitter);

ReScheduler.prototype = Object.create(ReScheduler.prototype, {
    // Enqueues payload for processing at the given date (a Date object, or a timestamp like
    // that from Date.getTime()).
    // The callback is called with 0 if the payload is already awaiting its execution date (in
    // which case the date is updated), or 1 if it is new.
    enqueueAt: {
        value: function(date, payload, callback) {
            var timestamp = (typeof date === 'number') ? date : date.getTime();
            this.client.zadd([ this.zsetName, timestamp, payload ], callback);
        },
        writable: true
    },

    // Same as enqueueAt, except with a relative time -- the given number of minutes from
    // the current time.
    enqueueIn: {
        value: function(minutes, payload, callback) {
            this.enqueueAt((new Date()).getTime() + minutes * 60 * 1000, payload, callback);
        },
        writable: true
    },

    // Calls back with the number of elements awaiting their execution date.
    scheduledCount: {
        value: function(callback) {
            this.client.zcard(this.zsetName, callback);
        },
        writable: true
    },

    // Looks for values whose execution time has come, and moves them onto targetList.
    // Calls back with the number of values moved to targetList.
    // This function is called automatically unless options.checkInterval was 0, or
    // stopChecking() was called.
    // If calling manually, make sure the Redis client provided is ready.
    checkNow: {
        value: function(callback) {
            var self = this,
                maxScore = (new Date()).getTime();

            if(!this.client.ready) {
                callback(new Error('Do not call checkNow() until the Redis client is ready'));
                return;
            }

            // If we're lucky, we can do all this server-side:
            if(this._usingServerEval) {
                // JSHint doesn't even like functions named eval :)
                /*jshint evil:true */
                this.client.eval([ luaScript, 2, this.zsetName, this.targetList, maxScore ], callback);
                return;
            }

            // Otherwise, grossness...
            // Pull everything out of the zset with scores up to the current time
            this.client.zrangebyscore([ this.zsetName, 0, maxScore ], function(err, scheduledPayloads) {
                if(err) {
                    if(callback) callback(err);
                    return;
                }

                if(scheduledPayloads.length === 0) {
                    if(callback) callback(null, 0);
                    return;
                }

                // Modify the payloads array so it's suitable as arguments for rpush, and push those payloads onto the target list.
                scheduledPayloads.unshift(self.targetList);
                self.client.rpush(scheduledPayloads, function(err) {
                    if(err) {
                        if(callback) callback(err);
                        return;
                    }

                    // And finally, remove the values we just processed from the set. Rejiggering the payloads array for zrem.
                    scheduledPayloads[0] = self.zsetName;
                    self.client.zrem(scheduledPayloads, function(err) {
                        if(callback) {
                            if(err) callback(err);
                            else callback(null, scheduledPayloads.length - 1);
                        }
                    });
                });
            });
        },
        writable: true
    },

    // Pops the next ready-to-be-executed value off targetList.
    // The callback is called with the value, or undefined if the timeout was reached with
    // no values having been pushed to the list.
    // timeout is optional and defaults to 0, which means "wait forever".
    // You must pass a different Redis client to this function than that passed to the
    // constructor of this instance.
    // This is a simple wrapper over the Redis blpop command.
    pop: {
        value: function(popClient, timeout, callback) {
            if(typeof timeout == 'function') {
                callback = timeout;
                timeout = 0;
            }

            if(popClient === this.client) {
                callback(new Error('The Redis client provided to pop() is the same as that being used for scheduling!'));
                return;
            }

            popClient.blpop([ this.targetList, timeout ], function(err, res) {
                if(callback) {
                    if(err) callback(err);
                    else callback(null, res[1]);  // blpop result is an array with the list key as the first element
                }
            });
        },
        writable: true
    },

    // Stop any automatic checking and optionally close the Redis client we were given.
    // If leaveClientOpen is true, you are free to use the client for other purposes
    // after calling this function.
    shutdown: {
        value: function(leaveClientOpen) {
            if(this.stopChecking) this.stopChecking();
            if(!!leaveClientOpen) this.client.quit();
        }
    },

    // Wrap all callback functions in Q promises.
    // You may optionally pass in the Q module, if "require('q')" will not provide the one
    // you wish.
    // After this function returns, all methods that take callbacks will return Q promises
    // for their results. This function returns `this`; it modifies the object in-place.
    // You may still call methods with a callback if you wish by simply pretending this
    // function was never called :)
    qWrap: {
        value: (function() {
            var wrapped = false;

            return function(Q) {
                if(!wrapped) {
                    var cbFns = [ 'enqueueAt', 'enqueueIn', 'scheduledCount', 'checkNow', 'pop' ],
                        i;

                    if(!Q) Q = require('q');  // This will throw if q isn't installed, which is fine.

                    for(i = 0; i < cbFns.length; i++)
                        promisify.call(this, cbFns[i], Q);

                    wrapped = true;
                }

                return this;
            };
        })()
    }
});

// Given an object and a function name on it, this rewrites that function such that if it's
// called with one less argument than it expects, it returns a Q promise for the call.
// If the function is called with the numer of arguments it expects, the original function
// is called as normal.
// That is: this makes fnName seamlessly deal with promises -- if you pass a callback, that will
// work. If you don't, the function will give you back a promise.
function promisify(fnName, Q) {
    /*jshint validthis:true */
    var originalFn = this[fnName],
        originalArity = originalFn.length;

    this[fnName] = function() {
        if(arguments.length === originalArity)
            return originalFn.apply(this, arguments);

        return Q.npost(this, fnName, arguments);
    };
}

function scheduleAutoCheck() {
    /*jshint validthis:true */
    var self = this,
        intervalId;

    intervalId = setInterval(function() {
        self.checkNow(function(err) {
            if(err) this.emit('error', err);
        });
    }, this.options.checkInterval);

    Object.defineProperty(this, 'stopChecking', { value: function() {
        if(intervalId) clearInterval(intervalId);
        intervalId = undefined;
    }});
}

function onReady() {
    /*jshint validthis:true */
    var serverVersion = this.client.server_info.versions,
        canUseEval;

    if(this.options.forceNoServerEval)
        canUseEval = false;
    else {
        if(serverVersion[0] > minVersionForEval[0]) canUseEval = true;
        else if(serverVersion[0] == minVersionForEval[0]) {
            if(serverVersion[1] >= minVersionForEval[1]) canUseEval = true;
            else if(serverVersion[1] == minVersionForEval[1]) {
                if(serverVersion[2] >= minVersionForEval[2]) canUseEval = true;
            }
        }
    }

    Object.defineProperty(this, '_usingServerEval', { value: canUseEval, enumerable: false });

    if(this.options.checkInterval > 0) scheduleAutoCheck.call(this);
}