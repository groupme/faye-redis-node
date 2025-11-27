// Constructor for multiRedis. It sets up two connections for each provided
// Redis URL and adds them to a ketama ring. One connection is used for
// commands and the other is used for pub/sub subscriptions.
//
// Updated for redis v4+ which uses Promises instead of callbacks.
var multiRedis = function(urls) {
  var hasher = require('consistent-hashing'),
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
  // Note: When multiple Redis servers are configured, this subscribes to all of them.
  // Messages are sharded by key, so each message only exists on one server.
  // This ensures we receive notifications regardless of which shard published them.
  subscribe: async function(topic, callback) {
    var self = this;

    for (var i = 0; i < self.urls.length; i++) {
      var url = self.urls[i];
      var subscription = self.subscriptions[url];

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
      console.error(label + ' Error:', err);
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
  parse: function(redisUrl) {
    var parsedUrl = new URL(redisUrl),
        connection = { hostname: parsedUrl.hostname, port: parseInt(parsedUrl.port, 10) || 6379 };

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
    return this.connections[this.ring.getNode(key)];
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
  // This implementation always overwrites existing members (does not use NX by default).
  // To enable NX behavior (only add if not exists), use: zAdd(key, { score, value }, { NX: true })
  zAdd: function(key, score, member) {
    return this.connectionFor(key).zAdd(key, { score: score, value: member });
  },

  zRem: function(key, member) {
    return this.connectionFor(key).zRem(key, member);
  },

  zScore: function(key, member) {
    return this.connectionFor(key).zScore(key, member);
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
  this._ensureInitialized();
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
          self.emptyQueue(message);
        });
      }

      if (self._options.gc) {
        if (process.env.STATSD_URL) {
          var statsd = require("node-statsd").StatsD;

          var statsdUrl = new URL(process.env.STATSD_URL);
          var prefix = "push." + process.env.NODE_ENV + ".";
          self.statsd = new statsd(statsdUrl.hostname, statsdUrl.port, prefix);
        }

        self.gc();
      }

      self._initialized = true;
      self._server.debug('Redis engine initialized successfully');
    } catch (error) {
      self._server.error('Failed to initialize Redis engine: ?', error);
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
 */
Engine.prototype.createClient = async function(callback, context) {
  await this._ensureInitialized();

  var clientId = this._server.generateId(),
      score = new Date().getTime(),
      self = this;

  try {
    var added = await this._redis.zAdd(this._ns + '/clients', score, clientId);
    if (added === 0) {
      return await self.createClient(callback, context);
    }
    self._server.debug('Created new client ? with score ?', clientId, score);
    self._server.trigger('handshake', clientId);
    if (callback) callback.call(context, clientId);
    return clientId;
  } catch (error) {
    self._server.error('Failed to create client: ?', error);
    throw error;
  }
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
    this._server.debug("[RedisEngine#clientExists] undefined clientId, returning false");
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
    this._server.error('Failed to check client existence: ?', error);
    if (callback) callback.call(context, false);
    return false;
  }
};

// Destroy a client.
//
// The first part of cleaning up a client is removing subscriptions, which
// removes the client ID from all the channels that it's a member of. This
// prevents messages from being published to that client.
//
// In a reversal of earlier behavior, callbacks are now _always_ called,
// but with an argument that indicates whether or not the destroy actually
// succeeded.
Engine.prototype.destroyClient = async function(clientId, callback, context) {
  await this._ensureInitialized();

  var self = this;
  var clientChannelsKey = this._ns + "/clients/" + clientId + "/channels";

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
    return self._failGC(callback, context, "Failed to fetch channels ?: ?", clientChannelsKey, error);
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

    self._server.debug("Destroyed client ? successfully", clientId);
    self._server.trigger("disconnect", clientId);

    if (self.statsd) {
      self.statsd.increment("gc.success");
    }

    if (callback) {
      callback.call(context, true);
    }
    return true;
  } catch (error) {
    return self._failGC(callback, context, "Failed to remove client ID ? from /clients: ?", clientId, error);
  }
};

Engine.prototype.ping = async function(clientId) {
  await this._ensureInitialized();

  var timeout = this._server.timeout;
  if (typeof timeout !== 'number') return;

  var time = new Date().getTime();

  try {
    this._server.debug('Ping ?, ?', clientId, time);
    await this._redis.zAdd(this._ns + '/clients', time, clientId);
  } catch (error) {
    this._server.error('Failed to ping client ?: ?', clientId, error);
    throw error;
  }
};

Engine.prototype.subscribe = async function(clientId, channel, callback, context) {
  await this._ensureInitialized();

  var self = this;

  try {
    var added = await this._redis.sAdd(this._ns + '/clients/' + clientId + '/channels', channel);
    if (added === 1) {
      self._server.trigger('subscribe', clientId, channel);
    }

    await this._redis.sAdd(this._ns + '/channels' + channel, clientId);
    self._server.debug('Subscribed client ? to channel ?', clientId, channel);

    if (callback) callback.call(context);
  } catch (error) {
    self._server.error('Failed to subscribe client: ?', error);
    if (callback) callback.call(context);
    throw error;
  }
};

Engine.prototype.unsubscribe = async function(clientId, channel, callback, context) {
  await this._ensureInitialized();

  var self = this;

  try {
    var removed = await this._redis.sRem(this._ns + '/clients/' + clientId + '/channels', channel);
    if (removed === 1) {
      self._server.trigger('unsubscribe', clientId, channel);
    }

    await this._redis.sRem(this._ns + '/channels' + channel, clientId);
    self._server.debug('Unsubscribed client ? from channel ?', clientId, channel);

    if (callback) callback.call(context);
  } catch (error) {
    self._server.error('Failed to unsubscribe client: ?', error);
    if (callback) callback.call(context);
    throw error;
  }
};

Engine.prototype.publish = async function(message, channels) {
  await this._ensureInitialized();

  this._server.debug('Publishing message ?', message);

  var self        = this,
      notified    = [],
      jsonMessage = JSON.stringify(message),
      keys        = channels.map(function(c) { return self._ns + '/channels' + c; });

  var notify = async function(clients) {
    for (var i = 0; i < clients.length; i++) {
      var clientId = clients[i];

      if (notified.indexOf(clientId) === -1) {
        var exists = await self.clientExists(clientId);

        if (exists) {
          self._server.debug('Queueing for client ?: ?', clientId, message);
          var messagesKey = self._ns + '/clients/' + clientId + '/messages';
          // Execute independent Redis operations in parallel for better performance
          await Promise.all([
            self._redis.rPush(messagesKey, jsonMessage),
            self._redis.publish(self._ns + '/notifications', clientId),
            self._redis.expire(messagesKey, 3600)
          ]);
          notified.push(clientId);
        } else {
          self._server.debug("Destroying expired client ? from publish", clientId);
          await self.destroyClient(clientId);
        }
      }
    }
  };

  for (var i = 0; i < keys.length; i++) {
    var key = keys[i];
    if (key.indexOf("*") === -1) {
      try {
        var clients = await self._redis.sMembers(key);
        await notify(clients);
      } catch (error) {
        self._server.error("Failed to fetch clients, candidate channels ?: ?", keys, error);
      }
    }
  }

  this._server.trigger('publish', message.clientId, message.channel, message.data);
};

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
    self._server.error('Failed to empty queue: ?', error);
  }
};

Engine.prototype.gc = function() {
  var timeout = this._server.timeout;
  if (typeof timeout !== 'number') return;

  var self = this;

  this._redis.urls.forEach(function(url) {
    self._server.debug("Starting GC loop for ?", url);
    process.nextTick(function() {
      self._runGC(url, timeout);
    });

    // Track the number of clients in each shard with a statsd gauge.
    if (self.statsd) {
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
    }
  });
};

Engine.prototype._runGC = async function(url, timeout) {
  var conn = this._redis.connections[url],
      cutoff = new Date().getTime() - 1000 * 2 * timeout,
      self = this;

  try {
    var clients = await conn.zRangeByScore(this._ns + "/clients", 0, cutoff, { LIMIT: { offset: 0, count: 1 } });

    if (clients.length === 0) {
      self._server.debug("[?] No GC clients, retrying in 2 seconds...", url);
      return setTimeout(self._runGC.bind(self), 2000, url, timeout);
    }

    var clientId = clients[0];
    var success = await self.destroyClient(clientId);

    if (success) {
      self._server.debug("[?] GC succeeded for ?", url, clientId);
    } else {
      self._server.warn("[?] GC failed for ?", url, clientId);
    }

    process.nextTick(function() {
      self._runGC(url, timeout);
    });
  } catch (error) {
    self._server.error("[?] Failed to fetch GC client, retrying in 2 seconds...", url);
    return setTimeout(self._runGC.bind(self), 2000, url, timeout);
  }
};

// A helper function to log a GC error and invoke the callback (if it exists).
Engine.prototype._failGC = function(callback, context, msg) {
  this._server.error.apply(this._server, Array.prototype.slice.call(arguments, 2, arguments.length));
  if (this.statsd) {
    this.statsd.increment("gc.failure");
  }
  if (callback) {
    callback.call(context, false);
  }
  return false;
};

module.exports = Engine;
