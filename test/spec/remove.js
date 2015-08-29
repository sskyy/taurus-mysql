var assert = require('assert')
var co = require('co')
var mongodb = require('mongodb')
var MongoClient = mongodb.MongoClient
var ObjectID = mongodb.ObjectID
var _ = require('lodash')
var util = require('../../lib/util')


function random(arr) {
  return arr[_.random(0, arr.length - 1)]
}

function fill( keys, value){
  return _.zipObject( keys, keys.map(function(){return value}))
}


describe('ensureCollection', function () {
  var database = 'mongodb://localhost:27017/test'
  var todosToInsert = ['swim', 'sing', 'dance', 'shot']
  var usersToInsert = [{
    name: 'john',
    age: 20,
    gender: 'male'
  }, {
    name: 'jason',
    age: 21,
    gender: 'male'
  }, {
    name: 'trinity',
    age: 22,
    gender: 'female'
  }]



  it('insert related data', function (done) {

    util.connect(database, function (db) {
      return co(function*() {


        var TodoCollection = db.collection('Todo')
        var UserCollection = db.collection('User')

        var todoResult = yield TodoCollection.insert({content: random(todosToInsert)})
        var userResult = yield UserCollection.insert([random(usersToInsert), random(usersToInsert)])

        //建立双向联系

        var insertedTodoIds = todoResult.ops.map(function( todo ){
          return new ObjectID(todo._id)
        })

        var insertedUserIds = userResult.ops.map(function( user){
          return new ObjectID( user._id )
        })

        yield Promise.all(insertedTodoIds.map(function (insertedTodoId) {
          return TodoCollection.update({_id: insertedTodoId}, {
            $set: {
              _mentioned_User: fill(insertedUserIds,true)
            }
          })
        }))

        yield Promise.all(insertedUserIds.map(function (insertedUserId) {
          return UserCollection.update({_id: insertedUserId}, {
            $set: {
              _Todo_mentioned: fill(insertedTodoIds,true)
            }
          })
        }))


        //尝试 query

        console.log( yield (TodoCollection.find({
            _id: {
              $in: insertedTodoIds
            }
          }).toArray()) )


        console.log( yield (UserCollection.find({
          _id: {
            $in: insertedUserIds
          }
        }).toArray()) )


        //尝试删除

        console.log(yield Promise.all(insertedTodoIds.map( function( insertedTodoId){
          return TodoCollection.findOneAndDelete({ _id : insertedTodoId })
        })))


        done()

      })
    }).catch(done)


  })
})