var assert = require('assert')
var co = require('co')
var Taurus = require('../../index.js')
var _ = require('lodash')
//var util = require('../../lib/util')
var print = require('pretty-log-2').pp



var taurus = new Taurus({
  host     : 'localhost',
  user     : 'root',
  socketPath  : '/tmp/mysql.sock',
})

describe('connect ', function () {

  it('connect and end', function( done ){

    return co(function *(){

      yield taurus.connect()

      yield taurus.end()
      done()

    }).catch(function(err){
      taurus.end()
      done(err)
    })

  })

})