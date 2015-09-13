var assert = require('assert')
var co = require('co')
var Taurus = require('../../index.js')
var _ = require('lodash')
//var util = require('../../lib/util')
var print = require('pretty-log-2').pp
var Galaxies = require('roof-zeroql/lib/Galaxies')


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

  it('new node', function( done ){

    //return co(function *(){
    //
    //  var clientId = 'abcdefg'
    //  var ast = {
    //    type : 'User',
    //    tracker : 'v0'
    //  }
    //
    //  var trackerRelationMap = {v0 : {}}
    //  trackerRelationMap.v0[clientId] = {id:clientId, type:'User'}
    //
    //  var nodesToSave = {'User':{}}
    //  nodesToSave.User[clientId] = {name:'jason'}
    //
    //  yield taurus.connect()
    //
    //  var clientServerIdMap = yield taurus.push( ast, nodesToSave, trackerRelationMap)
    //  print(clientServerIdMap)
    //
    //  yield taurus.end()
    //  done()
    //
    //}).catch(function(err){
    //  taurus.end()
    //  done(err)
    //})
done()
  })


  it('insert with relation node', function(done){
    var galaxies = new Galaxies(function( type, requestData ){
      return taurus.push( requestData.ast, requestData.rawNodesToSave, requestData.trackerRelationMap)
    }, types)

    var User = galaxies.getNodeClass('User')
    var Todo = galaxies.getNodeClass('Todo')

    var user = new User({name:'milk'})
    var todo = new Todo({content:'use some default milk'})

    return co(function *(){
      yield taurus.connect()

      user.relate(todo, 'created')
      yield user.push()

      yield taurus.end()
      console.log('=======')
      done()
    }).catch(function(err){
      taurus.end()
      done(err)
    })
  })
})