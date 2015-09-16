var assert = require('assert')
var co = require('co')
var Taurus = require('../../index.js')
var _ = require('lodash')
//var util = require('../../lib/util')
var print = require('pretty-log-2').pp
var Galaxies = require('roof-zeroql/lib/Galaxies')
var zeroQL = require('zeroQL')


var taurus = new Taurus({
  host     : 'localhost',
  user     : 'root',
  socketPath  : '/tmp/mysql.sock',
  database : 'taurus'
})

var types = [
  require('../common/types/user'),
  require('../common/types/todo')
]

describe('insert ', function () {


  it('pull', function(done){
    var galaxies = new Galaxies(function( type, requestData ){
      //console.log( {requestData})
      return co(function *(){
        var result = {}

        for( var i in requestData ){
          result[i] = yield taurus.pull( requestData[i])
        }

        return result
      })

    }, types)

    return co(function *(){
      yield taurus.connect()

      //console.log({test:zeroQL.parse(`User { created Todo {} }`).ast})
      var result= yield galaxies.sendQuery('test',{test:zeroQL.parse(`User { created Todo {} }`).ast})
      print(result.test.get().forEach(function(user){
        if( user.getRelative('created')){
          console.log( user.toObject())
          console.log(user.getRelative('created').toArray())
        }

      }))

      yield taurus.end()
      done()
    }).catch(function(err){
      taurus.end()
      done(err)
    })
  })
})