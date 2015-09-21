'use strict'
var assert = require('assert')
var co = require('co')
var Taurus = require('../../index.js')
var _ = require('lodash')
//var util = require('../../lib/util')
var print = require('pretty-log-2').pp
var Galaxies = require('roof-zeroql/lib/Galaxies')
var zeroQL = require('zeroQL')

var types = [
  require('../common/types/user'),
  require('../common/types/todo')
]

var taurus = new Taurus({
  host: 'localhost',
  user: 'root',
  socketPath: '/tmp/mysql.sock',
  database: 'taurus'
}, types)


describe('insert ', function () {


  it('pull', function (done) {
    var galaxies = new Galaxies(function (type, requestData) {
      //console.log( {requestData})
      return co(function *() {
        var result = {}

        for (var i in requestData) {
          result[i] = yield taurus.pull(requestData[i])
        }

        return result
      })

    }, types)

    return co(function *() {
      yield taurus.connect()

      //console.log({test:zeroQL.parse(`User { created Todo {} }`).ast})
      var entries = yield galaxies.sendQuery('test', {test: zeroQL.parse(`
      Todo(_limit:1,_total:true) {
          id,
          content,
          mentioned User(_total:true) {
            id,
            created Todo(_total:true) {
              id,
              content
            }
          }
        }
      `).ast})
      var nodes = entries['test'].get()
      console.log("nodes.total", nodes.total, nodes.length)
      //print( nodes.toArray() )
      nodes.forEach(node=>{
        console.log("should noly execute once!!!!!!")
        var mentiondUsers = node.getRelative('mentioned')
        if (mentiondUsers) {
          console.log(`Todo ${node.get('id')} mentioned User==>`)
          print( mentiondUsers.toArray())
          mentiondUsers.forEach(user=>{
            var createdTodos = user.getRelative('created')
            if( createdTodos ){
              console.log(`${user.get('id')} created=========>`)
              print( createdTodos.toArray())
            }
            //yield user.query.setFields(`id, name`)
          })

        }
        //yield taurus.end()
        //done()
      })

      yield taurus.end()
      done()
    }).catch(function (err) {
      taurus.end()
      done(err)
    })
  })
})