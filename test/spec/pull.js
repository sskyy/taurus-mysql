var assert = require('assert')
var co = require('co')
var mongodb = require('mongodb')
var MongoClient = mongodb.MongoClient
var ObjectID = mongodb.ObjectID
var _ = require('lodash')
var util = require('../../lib/util')
var pull = require('../../lib/collection').pull

var log = require('pretty-log-2').pp
var printTrackerMap = require('../common/printTrackerMap')


function random(arr) {
  return arr[_.random(0, arr.length - 1)]
}


describe('ensureCollection', function () {
  var database = 'mongodb://localhost:27017/test'


  it('pull', function (done) {

    util.connect(database, function (db) {
      return co(function*() {

        //var result = yield (pull({
        //  type : 'Todo',
        //  relations : {
        //    'mentioned_User' : {
        //      to : {
        //        type : 'User',
        //        relations : {}
        //      }
        //    }
        //  }
        //}))


        var result = yield (pull({
          "type": "Todo",
          "fields": [
          "id",
          "content"
        ],
          "attrs": {
          "data": {
            _limit : 2,

          },
          "unfilledKeys": []
        },
          "relations": {
          "mentioned_User": {
            "name": "mentioned",
              "to": {
              "reverse" : false,
              "type": "User",
                "fields": [
                "id",
                "name"
              ],
                "attrs": {
                "data": {
                  "_limit" : 1
                },
                "unfilledKeys": []
              },
              "relations": {},
              "tracker": "v1"
            },
            "static": false
          }
        },
          "tracker": "v0"
        }))

        //log( result.nodes )

        printTrackerMap(result.trackerRelationMap)


        done()

      })
    }).catch(done)


  })
})