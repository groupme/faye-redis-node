require('jsclass')
JS.require('JS.Range', 'JS.Test')

Faye = require('../vendor/faye/build/node/faye-node')
require('../vendor/faye/spec/javascript/engine_spec')
require('./faye_redis_spec')

// Faye.Logging.logLevel = 'debug';

JS.Test.autorun()
