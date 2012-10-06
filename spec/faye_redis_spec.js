var RedisEngine = require('../faye-redis')

JS.ENV.FayeRedisSpec = JS.Test.describe("Redis engine", function() { with(this) {
  before(function() {
    var pw = process.env.TRAVIS ? undefined : "foobared"
    this.engineOpts = {socket: '/tmp/redis.sock', type: RedisEngine, password: pw, namespace: new Date().getTime().toString()}
  })

  after(function(resume) { with(this) {
    sync(function() {
      engine.disconnect()
      var redis = require('redis').createClient(16379, 'localhost', {no_ready_check: true})
      redis.auth(engineOpts.password)
      redis.flushall(function() {
        redis.end()
        resume()
      })
    })
  }})

  itShouldBehaveLike("faye engine")

  describe("distribution", function() { with(this) {
    itShouldBehaveLike("distributed engine")
  }})

  if (process.env.TRAVIS) return

  describe("using a Unix socket", function() { with(this) {
    before(function() { with(this) {
      this.engineOpts.socket = "/tmp/redis.sock"
    }})

    itShouldBehaveLike("faye engine")
  }})
}})
