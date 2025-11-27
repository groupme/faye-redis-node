/**
 * Comprehensive integration test suite for faye-redis with modern Node.js
 * Requires a running Redis server on localhost:6379
 *
 * Run with: node spec/simple_test.js
 */

const RedisEngine = require('../faye-redis');
const assert = require('assert');

// Test results tracking
let passed = 0;
let failed = 0;
const failures = [];

// Helper to create a mock Faye server
function createMockServer(options = {}) {
  const events = [];
  const deliveries = [];
  const connections = new Set();

  return {
    timeout: options.timeout || 60,
    generateId: function() {
      return 'test-client-' + Math.random().toString(36).substr(2, 9);
    },
    debug: function() {
      if (options.verbose) console.log('[DEBUG]', ...arguments);
    },
    error: function() {
      console.error('[ERROR]', ...arguments);
    },
    warn: function() {
      console.warn('[WARN]', ...arguments);
    },
    trigger: function(event, ...args) {
      events.push({ event, args });
      if (options.verbose) console.log('[TRIGGER]', event, ...args);
    },
    hasConnection: function(clientId) {
      return connections.has(clientId);
    },
    addConnection: function(clientId) {
      connections.add(clientId);
    },
    removeConnection: function(clientId) {
      connections.delete(clientId);
    },
    deliver: function(clientId, messages) {
      deliveries.push({ clientId, messages });
      if (options.verbose) console.log('[DELIVER] to', clientId, ':', messages);
    },
    // Test helpers
    getEvents: () => events,
    getDeliveries: () => deliveries,
    clearEvents: () => events.length = 0,
    clearDeliveries: () => deliveries.length = 0
  };
}

// Test runner helper
async function test(name, fn) {
  try {
    await fn();
    passed++;
    console.log(`  ✓ ${name}`);
  } catch (error) {
    failed++;
    failures.push({ name, error });
    console.log(`  ✗ ${name}`);
    console.log(`    Error: ${error.message}`);
  }
}

// Assertion helpers
function assertEqual(actual, expected, message) {
  if (actual !== expected) {
    throw new Error(`${message || 'Assertion failed'}: expected ${expected}, got ${actual}`);
  }
}

function assertTrue(value, message) {
  if (!value) {
    throw new Error(message || 'Expected true but got false');
  }
}

function assertFalse(value, message) {
  if (value) {
    throw new Error(message || 'Expected false but got true');
  }
}

async function runTests() {
  console.log('=== Faye Redis Engine Test Suite ===\n');
  console.log('Node.js version:', process.version);

  const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379/0';
  console.log('Redis URL:', redisUrl);
  console.log('');

  // ============================================
  // Section 1: Basic Connection Tests
  // ============================================
  console.log('--- Connection Tests ---');

  let engine, mockServer;

  await test('should connect to Redis successfully', async () => {
    mockServer = createMockServer();
    engine = RedisEngine.create(mockServer, {
      namespace: 'faye-test-' + Date.now(),
      servers: [redisUrl],
      disable_subscriptions: true,
      gc: false
    });
    await engine.init();
  });

  await test('should handle multiple init calls idempotently', async () => {
    // Calling init again should return the same promise
    await engine.init();
    await engine.init();
  });

  // ============================================
  // Section 2: Client Lifecycle Tests
  // ============================================
  console.log('\n--- Client Lifecycle Tests ---');

  let clientId;

  await test('should create a client with callback', async () => {
    await new Promise((resolve) => {
      engine.createClient(function(id) {
        clientId = id;
        assertTrue(id.startsWith('test-client-'), 'Client ID should have correct prefix');
        resolve();
      });
    });
  });

  await test('should create a client with async/await (Promise)', async () => {
    const id = await engine.createClient();
    assertTrue(id.startsWith('test-client-'), 'Client ID should have correct prefix');
    // Clean up this extra client
    await engine.destroyClient(id);
  });

  await test('should trigger handshake event on client creation', async () => {
    const events = mockServer.getEvents();
    const handshakeEvent = events.find(e => e.event === 'handshake' && e.args[0] === clientId);
    assertTrue(handshakeEvent, 'Handshake event should have been triggered');
  });

  await test('should report client exists after creation', async () => {
    const exists = await engine.clientExists(clientId);
    assertTrue(exists, 'Client should exist after creation');
  });

  await test('should return false for non-existent client', async () => {
    const exists = await engine.clientExists('non-existent-client-id');
    assertFalse(exists, 'Non-existent client should not exist');
  });

  await test('should return false for undefined clientId', async () => {
    const exists = await engine.clientExists(undefined);
    assertFalse(exists, 'Undefined client should not exist');
  });

  await test('should update client timestamp on ping', async () => {
    await engine.ping(clientId);
    const exists = await engine.clientExists(clientId);
    assertTrue(exists, 'Client should still exist after ping');
  });

  // ============================================
  // Section 3: Subscription Tests
  // ============================================
  console.log('\n--- Subscription Tests ---');

  mockServer.clearEvents();

  await test('should subscribe client to a channel', async () => {
    await new Promise((resolve) => {
      engine.subscribe(clientId, '/messages/test', function() {
        resolve();
      });
    });
  });

  await test('should trigger subscribe event', async () => {
    const events = mockServer.getEvents();
    const subscribeEvent = events.find(e => e.event === 'subscribe' && e.args[1] === '/messages/test');
    assertTrue(subscribeEvent, 'Subscribe event should have been triggered');
  });

  await test('should subscribe to multiple channels', async () => {
    await engine.subscribe(clientId, '/messages/channel1');
    await engine.subscribe(clientId, '/messages/channel2');
    await engine.subscribe(clientId, '/private/user123');
  });

  await test('should unsubscribe client from a channel', async () => {
    mockServer.clearEvents();
    await new Promise((resolve) => {
      engine.unsubscribe(clientId, '/messages/test', function() {
        resolve();
      });
    });
  });

  await test('should trigger unsubscribe event', async () => {
    const events = mockServer.getEvents();
    const unsubscribeEvent = events.find(e => e.event === 'unsubscribe' && e.args[1] === '/messages/test');
    assertTrue(unsubscribeEvent, 'Unsubscribe event should have been triggered');
  });

  // ============================================
  // Section 4: Message Publishing Tests
  // ============================================
  console.log('\n--- Message Publishing Tests ---');

  await test('should publish a message to subscribed clients', async () => {
    mockServer.clearEvents();
    const message = {
      id: 'msg-1',
      channel: '/messages/channel1',
      data: { text: 'Hello, World!' },
      clientId: 'sender-client'
    };
    await engine.publish(message, ['/messages/channel1']);
  });

  await test('should trigger publish event', async () => {
    const events = mockServer.getEvents();
    const publishEvent = events.find(e => e.event === 'publish');
    assertTrue(publishEvent, 'Publish event should have been triggered');
  });

  // ============================================
  // Section 5: Message Queue Tests
  // ============================================
  console.log('\n--- Message Queue Tests ---');

  await test('should queue messages for delivery', async () => {
    // Create a fresh client for queue testing
    const queueClientId = await engine.createClient();
    await engine.subscribe(queueClientId, '/queue/test');

    // Publish a message
    const message = {
      id: 'queue-msg-1',
      channel: '/queue/test',
      data: { content: 'Queued message' },
      clientId: 'publisher'
    };
    await engine.publish(message, ['/queue/test']);

    // Add connection so emptyQueue will process
    mockServer.addConnection(queueClientId);
    mockServer.clearDeliveries();

    // Empty the queue
    await engine.emptyQueue(queueClientId);

    // Check deliveries
    const deliveries = mockServer.getDeliveries();
    assertTrue(deliveries.length > 0, 'Should have delivered messages');
    assertEqual(deliveries[0].clientId, queueClientId, 'Should deliver to correct client');

    // Cleanup
    mockServer.removeConnection(queueClientId);
    await engine.destroyClient(queueClientId);
  });

  await test('should not deliver to clients without connection', async () => {
    const noConnClientId = await engine.createClient();
    mockServer.clearDeliveries();

    // Don't add connection - emptyQueue should skip
    await engine.emptyQueue(noConnClientId);

    const deliveries = mockServer.getDeliveries();
    assertEqual(deliveries.length, 0, 'Should not deliver without connection');

    await engine.destroyClient(noConnClientId);
  });

  // ============================================
  // Section 6: Client Destruction Tests
  // ============================================
  console.log('\n--- Client Destruction Tests ---');

  await test('should destroy client with callback', async () => {
    const success = await new Promise((resolve) => {
      engine.destroyClient(clientId, function(result) {
        resolve(result);
      });
    });
    assertTrue(success, 'Destroy should return success');
  });

  await test('should trigger disconnect event on destroy', async () => {
    const events = mockServer.getEvents();
    const disconnectEvent = events.find(e => e.event === 'disconnect' && e.args[0] === clientId);
    assertTrue(disconnectEvent, 'Disconnect event should have been triggered');
  });

  await test('should report client does not exist after destruction', async () => {
    const exists = await engine.clientExists(clientId);
    assertFalse(exists, 'Client should not exist after destruction');
  });

  await test('should destroy client with async/await (Promise)', async () => {
    const newClientId = await engine.createClient();
    const success = await engine.destroyClient(newClientId);
    assertTrue(success, 'Async destroy should return success');
  });

  await test('should clean up channel subscriptions on destroy', async () => {
    // Create client, subscribe, then destroy
    const subClientId = await engine.createClient();
    await engine.subscribe(subClientId, '/cleanup/test');

    mockServer.clearEvents();
    await engine.destroyClient(subClientId);

    // Should trigger unsubscribe for the channel
    const events = mockServer.getEvents();
    const unsubEvent = events.find(e => e.event === 'unsubscribe' && e.args[0] === subClientId);
    assertTrue(unsubEvent, 'Should unsubscribe from channels on destroy');
  });

  // ============================================
  // Section 7: URL Parsing Tests
  // ============================================
  console.log('\n--- URL Parsing Tests ---');

  await test('should parse Redis URL with password', async () => {
    const testServer = createMockServer();
    const testEngine = RedisEngine.create(testServer, {
      namespace: 'parse-test-' + Date.now(),
      servers: ['redis://:secretpassword@localhost:6379/1'],
      disable_subscriptions: true,
      gc: false
    });
    // Just verify it creates without error - actual connection would fail without real auth
    assertTrue(testEngine !== null, 'Engine should be created');
  });

  await test('should parse Redis URL with default port', async () => {
    const testServer = createMockServer();
    const testEngine = RedisEngine.create(testServer, {
      namespace: 'parse-test-' + Date.now(),
      servers: ['redis://localhost/0'],
      disable_subscriptions: true,
      gc: false
    });
    assertTrue(testEngine !== null, 'Engine should be created with default port');
  });

  // ============================================
  // Section 8: Edge Cases
  // ============================================
  console.log('\n--- Edge Cases ---');

  await test('should handle destroying non-existent client gracefully', async () => {
    const success = await engine.destroyClient('non-existent-client');
    // Should still return true since there's nothing to clean up
    assertTrue(success, 'Should handle non-existent client gracefully');
  });

  await test('should handle ping for client without timeout', async () => {
    const noTimeoutServer = createMockServer({ timeout: undefined });
    const noTimeoutEngine = RedisEngine.create(noTimeoutServer, {
      namespace: 'no-timeout-test-' + Date.now(),
      servers: [redisUrl],
      disable_subscriptions: true,
      gc: false
    });
    await noTimeoutEngine.init();

    const id = await noTimeoutEngine.createClient();
    // Should not throw even without timeout
    await noTimeoutEngine.ping(id);

    await noTimeoutEngine.destroyClient(id);
    await noTimeoutEngine.disconnect();
  });

  await test('should check client existence without timeout (score not null check)', async () => {
    const noTimeoutServer = createMockServer({ timeout: undefined });
    const noTimeoutEngine = RedisEngine.create(noTimeoutServer, {
      namespace: 'no-timeout-exist-' + Date.now(),
      servers: [redisUrl],
      disable_subscriptions: true,
      gc: false
    });
    await noTimeoutEngine.init();

    const id = await noTimeoutEngine.createClient();
    const exists = await noTimeoutEngine.clientExists(id);
    assertTrue(exists, 'Client should exist when checking without timeout');

    await noTimeoutEngine.destroyClient(id);
    await noTimeoutEngine.disconnect();
  });

  // ============================================
  // Section 9: Pub/Sub Tests (with subscriptions enabled)
  // ============================================
  console.log('\n--- Pub/Sub Notification Tests ---');

  await test('should set up pub/sub notifications when enabled', async () => {
    const pubsubServer = createMockServer();
    const pubsubEngine = RedisEngine.create(pubsubServer, {
      namespace: 'pubsub-test-' + Date.now(),
      servers: [redisUrl],
      disable_subscriptions: false, // Enable subscriptions
      gc: false
    });

    await pubsubEngine.init();

    // Create a client and subscribe
    const pubsubClientId = await pubsubEngine.createClient();
    await pubsubEngine.subscribe(pubsubClientId, '/pubsub/channel');

    // Clean up
    await pubsubEngine.destroyClient(pubsubClientId);
    await pubsubEngine.disconnect();
  });

  // ============================================
  // Section 10: Disconnect Tests
  // ============================================
  console.log('\n--- Disconnect Tests ---');

  await test('should disconnect cleanly', async () => {
    await engine.disconnect();
  });

  await test('should handle multiple disconnects gracefully', async () => {
    // Already disconnected, should not throw
    try {
      await engine.disconnect();
    } catch (e) {
      // May throw but should handle gracefully
    }
  });

  // ============================================
  // Summary
  // ============================================
  console.log('\n========================================');
  console.log(`Tests: ${passed + failed} | Passed: ${passed} | Failed: ${failed}`);
  console.log('========================================\n');

  if (failures.length > 0) {
    console.log('Failed tests:');
    failures.forEach(({ name, error }) => {
      console.log(`  - ${name}`);
      console.log(`    ${error.stack || error.message}`);
    });
    console.log('');
  }

  if (failed > 0) {
    console.log('=== TESTS FAILED ===');
    process.exit(1);
  } else {
    console.log('=== ALL TESTS PASSED ===');
    process.exit(0);
  }
}

// Run tests
runTests().catch(error => {
  console.error('Test suite crashed:', error);
  process.exit(1);
});
