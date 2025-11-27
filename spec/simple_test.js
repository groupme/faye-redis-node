/**
 * Simple integration test for faye-redis with modern Node.js
 * Requires a running Redis server on localhost:6379
 *
 * Run with: node spec/simple_test.js
 */

const RedisEngine = require('../faye-redis');

// Mock Faye server object for testing
const mockServer = {
  timeout: 60,
  generateId: function() {
    return 'test-client-' + Math.random().toString(36).substr(2, 9);
  },
  debug: function() {
    console.log('[DEBUG]', ...arguments);
  },
  error: function() {
    console.error('[ERROR]', ...arguments);
  },
  warn: function() {
    console.warn('[WARN]', ...arguments);
  },
  trigger: function(event, ...args) {
    console.log('[TRIGGER]', event, ...args);
  },
  hasConnection: function(clientId) {
    return true;
  },
  deliver: function(clientId, messages) {
    console.log('[DELIVER] to', clientId, ':', messages);
  }
};

async function runTests() {
  console.log('=== Faye Redis Engine Test Suite ===\n');
  console.log('Node.js version:', process.version);
  console.log('');

  // Default Redis URL - can be overridden with REDIS_URL env var
  const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379/0';
  console.log('Using Redis URL:', redisUrl);
  console.log('');

  const options = {
    namespace: 'faye-test-' + Date.now(),
    servers: [redisUrl],
    disable_subscriptions: true, // Disable for simpler testing
    gc: false
  };

  const engine = RedisEngine.create(mockServer, options);

  try {
    console.log('1. Testing Redis connection...');
    await engine.init();
    console.log('   ✓ Connected to Redis successfully\n');

    console.log('2. Testing client creation...');
    let testClientId = null;
    await new Promise((resolve) => {
      engine.createClient(function(clientId) {
        testClientId = clientId;
        console.log('   ✓ Created client:', clientId);
        resolve();
      });
    });
    console.log('');

    console.log('3. Testing client existence check...');
    const exists = await engine.clientExists(testClientId);
    console.log('   ✓ Client exists:', exists);
    console.log('');

    console.log('4. Testing subscription...');
    await new Promise((resolve) => {
      engine.subscribe(testClientId, '/test/channel', function() {
        console.log('   ✓ Subscribed to /test/channel');
        resolve();
      });
    });
    console.log('');

    console.log('5. Testing ping...');
    await engine.ping(testClientId);
    console.log('   ✓ Ping sent successfully');
    console.log('');

    console.log('6. Testing unsubscription...');
    await new Promise((resolve) => {
      engine.unsubscribe(testClientId, '/test/channel', function() {
        console.log('   ✓ Unsubscribed from /test/channel');
        resolve();
      });
    });
    console.log('');

    console.log('7. Testing client destruction...');
    await new Promise((resolve) => {
      engine.destroyClient(testClientId, function(success) {
        console.log('   ✓ Client destroyed:', success);
        resolve();
      });
    });
    console.log('');

    console.log('8. Verifying client no longer exists...');
    const existsAfterDestroy = await engine.clientExists(testClientId);
    console.log('   ✓ Client exists after destroy:', existsAfterDestroy);
    console.log('');

    console.log('9. Testing disconnect...');
    await engine.disconnect();
    console.log('   ✓ Disconnected from Redis');
    console.log('');

    console.log('=== All tests passed! ===');
    process.exit(0);
  } catch (error) {
    console.error('\n✗ Test failed:', error);
    try {
      await engine.disconnect();
    } catch (e) {
      // Ignore disconnect errors during cleanup
    }
    process.exit(1);
  }
}

// Run tests
runTests();
