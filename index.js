/* jshint -W097 */
'use strict';

var events = require('events'),
    util = require('util'),
    luaScript;


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
// options is an optional hash. At present, the only option is 'checkInterval', the number of 
// milliseconds between checks for values whose execution time has arrived. It defaults to
// one minute. Provide a value of 0 to disable automatic checking; if you do this, it is up to
// you to intermittently call checkNow() to process values.
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
        key;

    function defineReadonlyProperty(name, value, enumerable) {
        Object.defineProperty(self, name, { value: value, enumerable: enumerable !== false });
    }

    defineReadonlyProperty('client', redisClient);
    defineReadonlyProperty('targetList', targetList);
    defineReadonlyProperty('zsetName', targetList + '-scheduler');

    var _options = { checkInterval: 60 * 1000 };
    for(key in _options) {
        if(options.hasOwnProperty(key) && options[key] !== undefined)
            _options[key] = options[key];
    }

    options = _options;
    console.log(options);
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
    checkNow: {
        value: function(callback) {
            var self = this,
                maxScore = (new Date()).getTime();

            this.client.eval([ luaScript, 2, this.zsetName, this.targetList, maxScore ], callback);
        },
        writable: true
    },

    // Pops the next ready-to-be-executed value off targetList.
    // The callback is called with the value, or undefined if the timeout was reached with
    // no values having been pushed to the list.
    // timeout is optional and defaults to 0, which means "wait forever".
    pop: {
        value: function(client, timeout, callback) {
            if(typeof timeout == 'function') {
                callback = timeout;
                timeout = 0;
            }

            client.blpop([ this.targetList, timeout ], function(err, res) {
                if(callback) {
                    if(err) callback(err);
                    else callback(null, res[1]);
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
                        promisify(this, cbFns[i], Q);

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
function promisify(rs, fnName, Q) {
    var originalFn = rs[fnName],
        originalArity = originalFn.length;

    rs[fnName] = function() {
        if(arguments.length === originalArity)
            return originalFn.apply(rs, arguments);

        return Q.npost(rs, fnName, arguments);
    };
}