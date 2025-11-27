/**
 * Integration test that simulates how Faye/push uses faye-redis
 * This tests the auto-initialization behavior required for Faye compatibility
 *
 * Run with: node spec/faye_integration_test.js
 */

const RedisEngine = require('../faye-redis');

// Simulate Faye's Engine.Proxy behavior
function FayeProxySimulator(options) {
  this.timeout = options.timeout || 60;
  this._connections = {};

  // This is what Faye does - it creates the engine synchronously
  // and then immediately starts using it
  var engineClass = options.type;
  this._engine = engineClass.create(this, options);

  console.log('[FayeProxy] Engine created');
}

FayeProxySimulator.prototype = {
  generateId: function() {
    return 'faye-client-' + Math.random().toString(36).slice(2, 11);
  },

  debug: function() {
    // Faye's debug logging
  },

  error: function() {
    console.error('[FayeProxy ERROR]', ...arguments);
  },

  warn: function() {
    console.warn('[FayeProxy WARN]', ...arguments);
  },

  trigger: function(event, ...args) {
    console.log('[FayeProxy TRIGGER]', event);
  },

  hasConnection: function(clientId) {
    return this._connections.hasOwnProperty(clientId);
  },

  deliver: function(clientId, messages) {
    console.log('[FayeProxy DELIVER]', clientId, messages.length, 'messages');
  },

  // Simulates Faye's connect method which calls ping and emptyQueue immediately
  connect: function(clientId, callback) {
    var self = this;
    console.log('[FayeProxy] connect() called for', clientId);

    // This is what Faye does - calls ping and emptyQueue right away
    this._engine.ping(clientId);
    this._connections[clientId] = true;
    this._engine.emptyQueue(clientId);

    // Simulate async callback
    setTimeout(function() {
      if (callback) callback();
    }, 10);
  },

  close: function() {
    return this._engine.disconnect();
  }
};

async function runIntegrationTest() {
  console.log('=== Faye Integration Test ===\n');
  console.log('Node.js version:', process.version);
  console.log('');

  const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379/0';
  console.log('Using Redis URL:', redisUrl);
  console.log('');

  // Simulate how push/bayeux.js creates the Faye adapter
  console.log('Step 1: Creating Faye-like proxy (simulates new faye.NodeAdapter())...');

  const proxy = new FayeProxySimulator({
    timeout: 600,
    type: RedisEngine,
    servers: [redisUrl],
    namespace: 'faye-integration-test-' + Date.now(),
    gc: false,
    disable_subscriptions: true
  });

  console.log('Step 2: Proxy created, engine is auto-initializing...');

  // Give the engine time to initialize
  // In real Faye, operations would queue/wait
  await new Promise(resolve => setTimeout(resolve, 500));

  console.log('Step 3: Testing createClient (like handshake)...');
  try {
    const clientId = await new Promise((resolve, reject) => {
      proxy._engine.createClient(function(id) {
        resolve(id);
      });
    });
    console.log('   ✓ Client created:', clientId);

    console.log('Step 4: Testing ping (happens on connect)...');
    proxy.connect(clientId, function() {
      console.log('   ✓ Connect callback fired');
    });

    // Wait for async operations
    await new Promise(resolve => setTimeout(resolve, 100));
    console.log('   ✓ Ping completed');

    console.log('Step 5: Testing subscribe...');
    await new Promise((resolve) => {
      proxy._engine.subscribe(clientId, '/test/channel', function() {
        console.log('   ✓ Subscribed to /test/channel');
        resolve();
      });
    });

    console.log('Step 6: Testing publish...');
    await proxy._engine.publish({
      id: 'msg-1',
      channel: '/test/channel',
      data: { text: 'Hello from integration test!' },
      clientId: 'publisher'
    }, ['/test/channel']);
    console.log('   ✓ Message published');

    console.log('Step 7: Testing unsubscribe...');
    await new Promise((resolve) => {
      proxy._engine.unsubscribe(clientId, '/test/channel', function() {
        console.log('   ✓ Unsubscribed from /test/channel');
        resolve();
      });
    });

    console.log('Step 8: Testing destroyClient (disconnect)...');
    await new Promise((resolve) => {
      proxy._engine.destroyClient(clientId, function(success) {
        console.log('   ✓ Client destroyed:', success);
        resolve();
      });
    });

    console.log('Step 9: Closing proxy...');
    await proxy.close();
    console.log('   ✓ Proxy closed');

    console.log('\n=== INTEGRATION TEST PASSED ===');
    process.exit(0);

  } catch (error) {
    console.error('\n✗ Integration test failed:', error);
    try {
      await proxy.close();
    } catch (e) {
      // Ignore
    }
    process.exit(1);
  }
}

// Run the test
runIntegrationTest();
