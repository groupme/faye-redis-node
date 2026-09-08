'use strict';

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const redis = require('redis');

const originalCreateClient = redis.createClient;
let createdClients = [];
let subscriptionCalls = [];

function FakeRedisClient() {
  this.handlers = Object.create(null);
  this.connectCalls = 0;
  this.quitCalls = 0;
}

FakeRedisClient.prototype.on = function() {};

FakeRedisClient.prototype.connect = async function() {
  this.connectCalls += 1;
};

FakeRedisClient.prototype.quit = async function() {
  this.quitCalls += 1;
};

FakeRedisClient.prototype.subscribe = async function(topic, callback) {
  subscriptionCalls.push({ client: this, topic: topic, callback: callback });
  this.handlers[topic] = callback;
};

redis.createClient = function() {
  const client = new FakeRedisClient();
  createdClients.push(client);
  return client;
};

const RedisEngine = require('../faye-redis');

function createServer(timeout) {
  return {
    timeout: timeout,
    generateId: function() {
      return 'test-client';
    },
    debug: function() {},
    error: function() {},
    trigger: function() {},
    hasConnection: function() {
      return false;
    },
    deliver: function() {}
  };
}

function engineOptions(overrides) {
  return Object.assign({
    namespace: 'review-test',
    servers: [
      'redis://localhost:6379/0',
      'redis://localhost:6380/0'
    ],
    disable_subscriptions: true,
    gc: false
  }, overrides || {});
}

function resetFakes() {
  createdClients = [];
  subscriptionCalls = [];
}

function delay(milliseconds) {
  return new Promise(function(resolve) {
    setTimeout(resolve, milliseconds);
  });
}

async function testNamedInitializationIsIdempotent() {
  resetFakes();
  const originalInitialization = RedisEngine.prototype._performInitialization;
  let initializationCalls = 0;

  RedisEngine.prototype._performInitialization = async function() {
    initializationCalls += 1;
    return originalInitialization.call(this);
  };

  let engine;
  try {
    engine = RedisEngine.create(createServer(60), engineOptions());
    const initialization = engine._initPromise;
    assert.strictEqual(typeof engine._redis._performInitialization, 'function');
    assert.strictEqual(engine.init(), initialization);
    assert.strictEqual(engine.init(), initialization);
    await initialization;
    assert.strictEqual(initializationCalls, 1);
    assert.strictEqual(engine._publishConcurrency, 50);
    assert.strictEqual(createdClients.length, 4);
    assert.ok(createdClients.every(function(client) {
      return client.connectCalls === 1;
    }));
  } finally {
    RedisEngine.prototype._performInitialization = originalInitialization;
    if (engine) await engine.disconnect();
  }
}

async function testRepeatedTopicSubscriptionsShareOneHandler() {
  resetFakes();
  const engine = RedisEngine.create(createServer(60), engineOptions());
  await engine.init();

  let firstCallbackCalls = 0;
  let secondCallbackCalls = 0;
  const first = engine._redis.subscribe('/repeated', function() {
    firstCallbackCalls += 1;
  });
  const second = engine._redis.subscribe('/repeated', function() {
    secondCallbackCalls += 1;
  });

  assert.strictEqual(first, second);
  await Promise.all([first, second]);
  await engine._redis.subscribe('/repeated', function() {
    secondCallbackCalls += 1;
  });

  const registrations = subscriptionCalls.filter(function(call) {
    return call.topic === '/repeated';
  });
  assert.strictEqual(registrations.length, 2);
  registrations.forEach(function(call) {
    call.callback('payload', '/repeated');
  });
  assert.strictEqual(firstCallbackCalls, 2);
  assert.strictEqual(secondCallbackCalls, 0);

  await engine.disconnect();
}

async function testPublishFanOutIsBounded() {
  resetFakes();
  const concurrency = 3;
  const engine = RedisEngine.create(
    createServer(60),
    engineOptions({ publish_concurrency: concurrency })
  );
  await engine.init();

  const clients = Array.from({ length: 12 }, function(_, index) {
    return 'client-' + index;
  });
  let active = 0;
  let maximumActive = 0;
  let scoreCalls = 0;

  engine._redis.sMembers = async function() {
    return clients;
  };
  engine._redis.zScore = async function() {
    scoreCalls += 1;
    active += 1;
    maximumActive = Math.max(maximumActive, active);
    await delay(5);
    active -= 1;
    return Date.now();
  };
  engine._redis.rPush = async function() {};
  engine._redis.publish = async function() {};
  engine._redis.expire = async function() {};

  await engine.publish(
    { clientId: 'publisher', channel: '/bounded', data: 'payload' },
    ['/bounded']
  );

  assert.strictEqual(scoreCalls, clients.length);
  assert.strictEqual(maximumActive, concurrency);
  await engine.disconnect();
}

async function testClientScoreNormalizationRejectsInvalidValues() {
  resetFakes();
  const server = createServer(60);
  const engine = RedisEngine.create(server, engineOptions());
  await engine.init();

  engine._redis.zScore = async function() {
    return String(Date.now());
  };
  assert.strictEqual(await engine.clientExists('numeric-string'), true);

  for (const invalidScore of ['not-a-number', '', Infinity, NaN, undefined, null]) {
    engine._redis.zScore = async function() {
      return invalidScore;
    };
    assert.strictEqual(await engine.clientExists('invalid-score'), false);
  }

  server.timeout = 0;
  engine._redis.zScore = async function() {
    return '123';
  };
  assert.strictEqual(await engine.clientExists('no-timeout'), true);

  await engine.disconnect();
}

async function testReleaseMetadataSignalsBreakingChanges() {
  const root = path.resolve(__dirname, '..');
  const packageJson = JSON.parse(fs.readFileSync(path.join(root, 'package.json')));
  const packageLock = JSON.parse(fs.readFileSync(path.join(root, 'package-lock.json')));
  const changelog = fs.readFileSync(path.join(root, 'CHANGELOG.txt'), 'utf8');

  assert.strictEqual(packageJson.version, '1.0.0');
  assert.strictEqual(packageLock.version, packageJson.version);
  assert.strictEqual(packageLock.packages[''].version, packageJson.version);
  assert.match(changelog, /^=== 1\.0\.0 /);
  assert.match(changelog, /BREAKING: Require Node\.js 18 or newer/);
  assert.match(changelog, /BREAKING: Upgrade to the Promise-based Redis 4 API/);
}

const tests = [
  ['named initialization is idempotent', testNamedInitializationIsIdempotent],
  ['repeated topic subscriptions share one handler', testRepeatedTopicSubscriptionsShareOneHandler],
  ['publish fan-out is bounded', testPublishFanOutIsBounded],
  ['client scores are normalized and validated', testClientScoreNormalizationRejectsInvalidValues],
  ['release metadata signals breaking changes', testReleaseMetadataSignalsBreakingChanges]
];

async function run() {
  try {
    for (const testCase of tests) {
      await testCase[1]();
      console.log('✓ ' + testCase[0]);
    }
  } finally {
    redis.createClient = originalCreateClient;
  }
}

run().catch(function(error) {
  console.error(error.stack || error);
  process.exitCode = 1;
});
