// Constructor for multiRedis. It sets up two connections for each provided
// Redis URL and adds them to a ketama ring. One connection is used for
// commands and the other is used for pub/sub subscriptions.
//
// Updated for redis v4+ which uses Promises instead of callbacks.
var multiRedis = function(urls) {
  var hasher = require('hashring'),
      self   = this;

  self.ring          = new hasher(urls);
  self.urls          = urls;
  self.connections   = {};
  self.subscriptions = {};
  self._ready        = false;
  self._readyPromise = null;
};

multiRedis.prototype = {
  // Initialize all Redis connections (must be called before using the client)
  init: async function() {
    var self = this;

    if (self._readyPromise) {
      return self._readyPromise;
    }

    self._readyPromise = (async function() {
      for (var i = 0; i < self.urls.length; i++) {
        var url = self.urls[i];
        var options = self.parse(url);

        var connection = await self.connect(options);
        var subscription = await self.connectSubscriber(options);

        self.connections[url] = connection;
        self.subscriptions[url] = subscription;
      }
      self._ready = true;
    })();

    return self._readyPromise;
  },

  // Grab the connection from the ring for the pub/sub server for the message
  // and delegate a publish call to it.
  publish: async function(topic, message) {
    var connection = this.connectionFor(message);
    return connection.publish(topic, message);
  },

  // Subscribe to the topic on all of the subscription connections and call
  // the callback on a new message.
  //
  // Note: When multiple Redis servers are configured, this subscribes to all of them.
  // Messages are sharded by key, so each message only exists on one server.
  // This ensures we receive notifications regardless of which shard published them.
  //
  // IMPORTANT: This method should only be called once per topic. Calling it multiple
  // times will register multiple handlers and cause duplicate message processing.
  //
  // The callback signature is (channel, message) to match the original Faye API.
  // Redis v4+ provides (message, channel), so we swap the arguments.
  subscribe: async function(topic, callback) {
    var self = this;

    for (var i = 0; i < self.urls.length; i++) {
      var url = self.urls[i];
      var subscription = self.subscriptions[url];

      // Redis v4+ callback is (message, channel), but Faye expects (channel, message)
      await subscription.subscribe(topic, function(message, channel) {
        callback(channel, message);
      });
    }
  },

  // Returns a multi/transaction object for the connection that handles the given key.
  // Use this when you need to execute multiple commands atomically on the same shard.
  // Note: In redis v4+, multi() takes no arguments; the key is only used to select the connection.
  multi: function(key) {
    return this.connectionFor(key).multi();
  },

  // Returns a new Redis connection. Expects a server configuration object,
  // e.g.:
  //
  //   { port: 6379,
  //   hostname: 'localhost',
  //   database: 0,
  //   password: 'chunkybacon' }
  connect: async function(server, errorLabel) {
    var redis = require('redis');
    var label = errorLabel || 'Redis Client';

    var clientOptions = {
      socket: {
        host: server.hostname,
        port: server.port
      },
      database: parseInt(server.database, 10) || 0
    };

    if (server.password) {
      clientOptions.password = server.password;
    }

    var client = redis.createClient(clientOptions);

    client.on('error', function(err) {
      this._server.error(label + ' Error:', err);
    });

    await client.connect();
    return client;
  },

  // Creates a subscriber connection (separate from command connection in redis v4+)
  connectSubscriber: function(server) {
    return this.connect(server, 'Redis Subscriber');
  },

  // Parses a URL and returns a server configuration object, e.g.:
  //
  // redis://:chunkybacon@localhost:6379/0
  //
  // Throws an error if the URL is malformed.
  parse: function(redisUrl) {
    var parsedUrl;
    try {
      parsedUrl = new URL(redisUrl);
    } catch (e) {
      throw new Error('Invalid Redis URL: ' + redisUrl + ' - ' + e.message);
    }

    var connection = { hostname: parsedUrl.hostname, port: parseInt(parsedUrl.port, 10) || 6379 };

    if (parsedUrl.password) {
      connection.password = parsedUrl.password;
    }

    if (parsedUrl.pathname && parsedUrl.pathname.length > 1) {
      connection.database = parsedUrl.pathname.substring(1);
    } else {
      connection.database = 0;
    }

    return connection;
  },

  // Closes all connections to Redis.
  end: async function() {
    var self = this;

    for (var i = 0; i < self.urls.length; i++) {
      var url = self.urls[i];

      try {
        if (self.connections[url]) {
          await self.connections[url].quit();
        }
      } catch (e) {
        // Connection may already be closed
      }

      try {
        if (self.subscriptions[url]) {
          await self.subscriptions[url].quit();
        }
      } catch (e) {
        // Connection may already be closed
      }
    }
  },

  // Returns a connection for a given key.
  connectionFor: function(key) {
    return this.connections[this.ring.get(key)];
  },

  // Redis v4+ command wrappers with proper sharding
  // Note: redis v4+ uses camelCase method names

  sMembers: function(key) {
    return this.connectionFor(key).sMembers(key);
  },

  del: function(key) {
    return this.connectionFor(key).del(key);
  },

  sAdd: function(key, member) {
    return this.connectionFor(key).sAdd(key, member);
  },

  sRem: function(key, member) {
    return this.connectionFor(key).sRem(key, member);
  },

  rPush: function(key, value) {
    return this.connectionFor(key).rPush(key, value);
  },

  expire: function(key, seconds) {
    return this.connectionFor(key).expire(key, seconds);
  },

  get: function(key) {
    return this.connectionFor(key).get(key);
  },

  // zAdd signature in v4+: zAdd(key, { score, value }) or zAdd(key, [{ score, value }])
  //
  // Return value: Returns the number of NEW elements added to the sorted set.
  // If the member already exists, its score is updated but the return value is 0.
  // This is important for collision detection in createClient().
  //
  // This implementation always overwrites existing scores (does not use NX by default).
  // To enable NX behavior (only add if not exists), use: zAdd(key, { score, value }, { NX: true })
  //
  // IMPORTANT: Hashes on 'member' (argument 2) to maintain compatibility with old code
  // and Go implementation. This ensures all operations on the same clientId hit the same shard.
  zAdd: function(key, score, member) {
    return this.connectionFor(member).zAdd(key, { score: score, value: member });
  },

  // IMPORTANT: Hashes on 'member' (argument 1) to maintain compatibility with old code
  // and Go implementation. This ensures all operations on the same clientId hit the same shard.
  zRem: function(key, member) {
    return this.connectionFor(member).zRem(key, member);
  },

  // IMPORTANT: Hashes on 'member' (argument 1) to maintain compatibility with old code
  // and Go implementation. This ensures all operations on the same clientId hit the same shard.
  zScore: function(key, member) {
    return this.connectionFor(member).zScore(key, member);
  }
};

// Creates a new Faye Redis engine.
//
// Options:
//   disable_subscriptions If set to `true`, then this engine will not subscribe
//                         to the notifications channel.
//
//   gc                    When `true`, GC is run continuously in this process.
//                         Seeing as how it's no longer interval-based, you
//                         probably only want to set this in a dedicated GC
//                         process.
//
var Engine = function(server, options) {
  this._options = options || {};

  var self = this;

  this._server     = server;
  this._ns         = this._options.namespace || '';
  this._redis      = new multiRedis(options.servers);
  this._initialized = false;
  this._initPromise = null;
  this._gcIntervals = [];

  // Auto-initialize on construction (for Faye compatibility)
  // This starts the async connection process immediately
  this._ensureInitialized().catch(err => this._server.error('Auto-initialization failed:', err));
};

Engine.create = function(server, options) {
  return new this(server, options);
};

// Ensures the engine is initialized, starting initialization if needed.
// Returns a promise that resolves when initialization is complete.
Engine.prototype._ensureInitialized = function() {
  var self = this;

  if (this._initPromise) {
    return this._initPromise;
  }

  this._initPromise = (async function() {
    try {
      await self._redis.init();

      if (!self._options.disable_subscriptions) {
        await self._redis.subscribe(self._ns + '/notifications', function(topic, message) {
          self.emptyQueue(message).catch(err => self._server.error('[faye-redis] Failed to empty queue on notification:', err));
        });
      }

      if (self._options.gc) {
        if (process.env.DD_AGENT_HOST) {
          try {
            var statsd = require("node-statsd").StatsD;

            var prefix = "push." + process.env.NODE_ENV + ".";
            self.statsd = new statsd(process.env.DD_AGENT_HOST, 8125, prefix);
          } catch (e) {
            self._server.error('[faye-redis] Failed to initialize StatsD: ' + e.message);
          }
        }

        self.gc();
      }

      self._initialized = true;
      self._server.debug('[faye-redis] Redis engine initialized successfully');
    } catch (error) {
      self._server.error('[faye-redis] Failed to initialize Redis engine:', error);
      throw error;
    }
  })();

  return this._initPromise;
};

// Public init method for explicit initialization (also used by tests)
Engine.prototype.init = function() {
  return this._ensureInitialized();
};

Engine.prototype.DEFAULT_GC = 60;
Engine.prototype.LOCK_TIMEOUT = 120;

Engine.prototype.disconnect = async function() {
  await this._redis.end();
  if (this._gcIntervals) {
    this._gcIntervals.forEach(function(intervalId) {
      clearInterval(intervalId);
    });
    this._gcIntervals = [];
  }
};

/**
 * Creates a new client and registers it with the server.
 * @param {Function} [callback] - DEPRECATED: Use the returned Promise instead.
 *                                Called with (clientId) on success.
 * @param {Object} [context] - DEPRECATED: The context for the callback.
 * @returns {Promise<string>} The new client ID.
 * @throws {Error} If a unique client ID cannot be generated after 10 attempts.
 */
Engine.prototype.createClient = async function(callback, context) {
  await this._ensureInitialized();

  var self = this;
  // Maximum retry attempts for client ID collision.
  // With random 9-character IDs, collisions are extremely rare.
  // 10 retries provides ample safety margin.
  var maxRetries = 10;

  for (var attempt = 0; attempt < maxRetries; attempt++) {
    var clientId = this._server.generateId();
    var score = new Date().getTime();

    try {
      var added = await this._redis.zAdd(this._ns + '/clients', score, clientId);
      if (added === 1) {
        self._server.debug('[faye-redis] Created new client ' + clientId + ' with score ' + score);
        self._server.trigger('handshake', clientId);
        if (callback) callback.call(context, clientId);
        return clientId;
      }
      // added === 0 means clientId already exists (collision), try again
      self._server.debug('[faye-redis] Client ID collision, retrying... attempt ' + (attempt + 1));
    } catch (error) {
      self._server.error('[faye-redis] Failed to create client:', error);
      throw error;
    }
  }

  var error = new Error('Failed to create unique client ID after ' + maxRetries + ' attempts');
  self._server.error('[faye-redis] Failed to create client:', error);
  throw error;
};

/**
 * Checks if a client exists and is not expired.
 * @param {string} clientId - The client ID to check.
 * @param {Function} [callback] - DEPRECATED: Use the returned Promise instead.
 *                                Called with (exists: boolean).
 * @param {Object} [context] - DEPRECATED: The context for the callback.
 * @returns {Promise<boolean>} Whether the client exists.
 */
Engine.prototype.clientExists = async function(clientId, callback, context) {
  await this._ensureInitialized();

  var timeout = this._server.timeout;

  if (clientId === undefined) {
    this._server.debug("[faye-redis] [RedisEngine#clientExists] undefined clientId, returning false");
    if (callback) callback.call(context, false);
    return false;
  }

  try {
    var score = await this._redis.zScore(this._ns + '/clients', clientId);
    var exists;
    if (timeout) {
      exists = score !== null && score > new Date().getTime() - 1000 * 1.75 * timeout;
    } else {
      exists = score !== null;
    }
    if (callback) callback.call(context, exists);
    return exists;
  } catch (error) {
    this._server.error('[faye-redis] Failed to check client existence:', error.message);
    if (callback) callback.call(context, false);
    return false;
  }
};

/**
 * Destroys a client and cleans up all associated data.
 * Removes channel subscriptions, message queue, and client record.
 * @param {string} clientId - The client ID to destroy.
 * @param {Function} [callback] - DEPRECATED: Use the returned Promise instead.
 *                                Called with (success: boolean).
 * @param {Object} [context] - DEPRECATED: The context for the callback.
 * @returns {Promise<boolean>} Whether the destroy succeeded.
 */
Engine.prototype.destroyClient = async function(clientId, callback, context) {
  await this._ensureInitialized();

  var self = this;
  var clientChannelsKey = this._ns + "/clients/" + clientId + "/channels";
  self._server.debug("[faye-redis] Destroying client " + clientId);

  try {
    var channels = await this._redis.sMembers(clientChannelsKey);

    if (channels.length === 0) {
      return self._deleteClient(clientId, callback, context);
    }

    var unsubscribePromises = channels.map(async function(channel) {
      var channelsKey = self._ns + "/channels" + channel;
      await self._redis.sRem(channelsKey, clientId);
      self._server.trigger("unsubscribe", clientId, channel);
    });

    await Promise.all(unsubscribePromises);
    return self._deleteClient(clientId, callback, context);
  } catch (error) {
    return self._failGC(callback, context, "[faye-redis] Failed to fetch channels " + clientChannelsKey + ": " + error.message);
  }
};

// Removes the client bookkeeping records.
//
// Finishes client cleanup by removing the mailbox, channel set, and finally
// the client ID from the sorted set. Once again, any Redis errors shut down
// the callback chain, and we'll rely on GC to pick it back up again.
Engine.prototype._deleteClient = async function(clientId, callback, context) {
  var self = this,
      clientChannelsKey = this._ns + "/clients/" + clientId + "/channels",
      clientMessagesKey = this._ns + "/clients/" + clientId + "/messages";

  try {
    // Execute independent Redis delete operations in parallel for better performance
    await Promise.all([
      this._redis.del(clientChannelsKey),
      this._redis.del(clientMessagesKey),
      this._redis.zRem(self._ns + "/clients", clientId)
    ]);

    self._server.debug("[faye-redis] Destroyed client " + clientId + " successfully");
    self._server.trigger("disconnect", clientId);

    if (self.statsd) {
      self.statsd.increment("gc.success");
    }

    if (callback) {
      callback.call(context, true);
    }
    return true;
  } catch (error) {
    return self._failGC(callback, context, "[faye-redis] Failed to remove client ID " + clientId + " from /clients: " + (error && error.message ? error.message : String(error)));
  }
};

/**
 * Updates the client's last-seen timestamp.
 * @param {string} clientId - The client ID to ping.
 * @returns {Promise<void>}
 */
Engine.prototype.ping = async function(clientId) {
  await this._ensureInitialized();

  var timeout = this._server.timeout;
  if (typeof timeout !== 'number') return;

  var time = new Date().getTime();

  try {
    this._server.debug('[faye-redis] Ping ' + clientId + ', ' + time);
    await this._redis.zAdd(this._ns + '/clients', time, clientId);
  } catch (error) {
    this._server.error('[faye-redis] Failed to ping client ' + clientId + ':', error);
    throw error;
  }
};

/**
 * Subscribes a client to a channel.
 * @param {string} clientId - The client ID.
 * @param {string} channel - The channel to subscribe to.
 * @param {Function} [callback] - DEPRECATED: Use the returned Promise instead.
 * @param {Object} [context] - DEPRECATED: The context for the callback.
 * @returns {Promise<void>}
 */
Engine.prototype.subscribe = async function(clientId, channel, callback, context) {
  await this._ensureInitialized();

  var self = this;

  try {
    var added = await this._redis.sAdd(this._ns + '/clients/' + clientId + '/channels', channel);
    if (added === 1) {
      self._server.trigger('subscribe', clientId, channel);
    }

    await this._redis.sAdd(this._ns + '/channels' + channel, clientId);
    self._server.debug('[faye-redis] Subscribed client ' + clientId + ' to channel ' + channel);

    if (callback) callback.call(context);
  } catch (error) {
    self._server.error('[faye-redis] Failed to subscribe client:', error);
    // Don't call callback on error - let the thrown error propagate to Promise-based callers
    throw error;
  }
};

/**
 * Unsubscribes a client from a channel.
 * @param {string} clientId - The client ID.
 * @param {string} channel - The channel to unsubscribe from.
 * @param {Function} [callback] - DEPRECATED: Use the returned Promise instead.
 * @param {Object} [context] - DEPRECATED: The context for the callback.
 * @returns {Promise<void>}
 */
Engine.prototype.unsubscribe = async function(clientId, channel, callback, context) {
  await this._ensureInitialized();

  var self = this;

  try {
    var removed = await this._redis.sRem(this._ns + '/clients/' + clientId + '/channels', channel);
    if (removed === 1) {
      self._server.trigger('unsubscribe', clientId, channel);
    }

    await this._redis.sRem(this._ns + '/channels' + channel, clientId);
    self._server.debug('[faye-redis] Unsubscribed client ' + clientId + ' from channel ' + channel);

    if (callback) callback.call(context);
  } catch (error) {
    self._server.error('[faye-redis] Failed to unsubscribe client:', error);
    // Don't call callback on error - let the thrown error propagate to Promise-based callers
    throw error;
  }
};

/**
 * Publishes a message to all subscribed clients on the given channels.
 * @param {Object} message - The message to publish.
 * @param {string[]} channels - The channels to publish to.
 * @returns {Promise<void>}
 */
Engine.prototype.publish = async function(message, channels) {
  await this._ensureInitialized();

  var self        = this,
      notified    = new Set(),
      jsonMessage = JSON.stringify(message),
      keys        = channels.map(function(c) { return self._ns + '/channels' + c; });

  self._server.debug("[faye-redis] Publishing message to channels:", message, channels);
  var notifyClient = async function(clientId) {
    if (notified.has(clientId)) {
      return;
    }
    notified.add(clientId);

    var exists = await self.clientExists(clientId);

    if (exists) {
      self._server.debug('[faye-redis] Queueing for client ' + clientId + ':', JSON.stringify(message));
      var messagesKey = self._ns + '/clients/' + clientId + '/messages';
      // Execute independent Redis operations in parallel for better performance
      await Promise.all([
        self._redis.rPush(messagesKey, jsonMessage),
        self._redis.publish(self._ns + '/notifications', clientId),
        self._redis.expire(messagesKey, 3600)
      ]);
    } else {
      self._server.debug("[faye-redis] Destroying expired client " + clientId + " from publish");
      await self.destroyClient(clientId);
    }
  };

  for (var i = 0; i < keys.length; i++) {
    var key = keys[i];
    if (key.indexOf("*") === -1) {
      try {
        var clients = await self._redis.sMembers(key);
        // Process clients in parallel for better performance
        await Promise.all(clients.map(notifyClient));
      } catch (error) {
        self._server.error("[faye-redis] Failed to fetch clients for channels " + keys.join(', ') + ": " + error.message);
      }
    }
  }

  this._server.trigger('publish', message.clientId, message.channel, message.data);
};

/**
 * Delivers queued messages to a connected client.
 * @param {string} clientId - The client ID to deliver messages to.
 * @returns {Promise<void>}
 */
Engine.prototype.emptyQueue = async function(clientId) {
  await this._ensureInitialized();

  if (!this._server.hasConnection(clientId)) return;

  var key = this._ns + '/clients/' + clientId + '/messages',
      self = this;

  try {
    var conn = this._redis.connectionFor(key);
    var multi = conn.multi();
    multi.lRange(key, 0, -1);
    multi.del(key);

    var results = await multi.exec();
    var jsonMessages = results[0] || [];
    var messages = jsonMessages.map(function(json) { return JSON.parse(json); });
    self._server.deliver(clientId, messages);
  } catch (error) {
    self._server.error('[faye-redis] Failed to empty queue:', error);
  }
};

Engine.prototype.gc = function() {
  var timeout = this._server.timeout;
  if (typeof timeout !== 'number') return;

  var self = this;

  this._redis.urls.forEach(function(url) {
    self._server.debug("[faye-redis] Starting GC loop for " + url);
    process.nextTick(function() {
      self._runGC(url, timeout).catch(function(err) {
        self._server.error('[faye-redis] GC error:', err);
      });
    });

    // Track the number of clients in each shard with a statsd gauge.
    if (self.statsd) {
      try {
        var host = new URL(url).hostname.replace(/\./g, '_'),
            conn = self._redis.connections[url],
            statKey = "clients." + host;

        var intervalId = setInterval(async function() {
          try {
            var n = await conn.zCard(self._ns + "/clients");
            self.statsd.gauge(statKey, n);
          } catch (error) {
            // Ignore errors
          }
        }, 10000);
        self._gcIntervals.push(intervalId);
      } catch (e) {
        self._server.error('[faye-redis] Failed to parse URL for stats: ' + e.message);
      }
    }
  });
};

Engine.prototype._runGC = async function(url, timeout) {
  var conn = this._redis.connections[url],
      cutoff = new Date().getTime() - 1000 * 2 * timeout,
      self = this;

  self._server.debug("[faye-redis] [" + url + "] _runGC called, cutoff: " + cutoff);

  try {
    var clients = await conn.zRangeByScore(this._ns + "/clients", 0, cutoff, { LIMIT: { offset: 0, count: 1 } });

    self._server.debug("[faye-redis] [" + url + "] Found " + clients.length + " expired clients");

    if (clients.length === 0) {
      self._server.debug("[faye-redis] [" + url + "] No GC clients, retrying in 2 seconds...");
      return setTimeout(self._runGC.bind(self), 2000, url, timeout);
    }

    var clientId = clients[0];
    self._server.debug("[faye-redis] [" + url + "] Attempting to destroy client: " + clientId);

    var success = await self.destroyClient(clientId);

    if (success) {
      self._server.debug("[faye-redis] [" + url + "] GC succeeded for " + clientId);
    } else {
      self._server.error("[faye-redis] [" + url + "] GC failed for " + clientId);
    }

    process.nextTick(function() {
      self._runGC(url, timeout).catch(err => self._server.error('[faye-redis] GC error:', err));
    });
  } catch (error) {
    self._server.error("[faye-redis] [" + url + "] Failed to fetch GC client: " + error.message + ", retrying in 2 seconds...");
    return setTimeout(self._runGC.bind(self), 2000, url, timeout);
  }
};

// A helper function to log a GC error and invoke the callback (if it exists).
Engine.prototype._failGC = function(callback, context, msg) {
  // Prepend [faye-redis] to the message
  var args = Array.prototype.slice.call(arguments, 2, arguments.length);
  if (args.length > 0 && typeof args[0] === 'string') {
    args[0] = '[faye-redis] ' + args[0];
  }
  this._server.error.apply(this._server, args);
  if (this.statsd) {
    this.statsd.increment("gc.failure");
  }
  if (callback) {
    callback.call(context, false);
  }
  return false;
};

module.exports = Engine;
