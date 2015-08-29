var _ = require('lodash')
var util = require('./util')
var mongodb = require('mongodb')
var ObjectID = mongodb.ObjectID
var log = require('pretty-log-2').pp
var co = require('co')

var database = 'mongodb://localhost:27017/test'

function safeMongoKey( str ){
  return `_R_${str.replace('-','_')}`
}

function isRelationKey( str ){
  return /^_R_/.test( str )
}

function reverseRelationKey( str, type ){
  var tmp = str.replace(/^_R_/,'').split('_')
  //判断哪个是type
  var reverse = /^[A-Z]/.test( tmp[0] )
  var relationName = reverse  ? tmp[1] : tmp[0]

  return `_R_${ (reverse ? [relationName, type] : [type, relationName]).join('_')}`
}



////////////////////////////////////////////////////////////////////////////////
//     destroy
////////////////////////////////////////////////////////////////////////////////
//单个节点
//TODO 改善性能
function *destroy( type,id ){
  console.log( "tring to destroy",type, id)
  console.log("instanceof",id instanceof ObjectID)

  id = (id instanceof ObjectID) ? id : ObjectID(id)
console.log("-----")
  return util.connect( database, function(db){
    return db.collection(type).findOneAndDelete({_id:id}).then(function(result){
      //TODO 判断 results 是否为空

      var node = result.value
      console.log('deleted', node)
      return Promise.all( Object.keys(node).map(function(key){
        if( !isRelationKey(key)) return true

        return Promise.all( Object.keys(node[key]).map(function( relatedId){
          var toUnset = {}
          var unsetKey = `${reverseRelationKey(key, type)}.${node._id}`
          toUnset[unsetKey] = ''

          console.log('updating related', key, relatedId, node[key][relatedId].type, toUnset)
          return db.collection( node[key][relatedId].type ).update(
            {_id : ObjectID(relatedId)},
            {$unset :toUnset}
            //{$set : toUnset}
          )

        }))
      })).then(function(){
        return node
      })
    })
  })
}

////////////////////////////////////////////////////////////////////////////////
//     pull
////////////////////////////////////////////////////////////////////////////////
//多个节点
function *pull( ast ){

  //不需要结果树

  var result = {
    nodes : {},
    trackerRelationMap : {
    }
  }

  var trackerNodesCache = {}


  console.log( 'pulling')
  log(ast)

  return util.connect( database, function(db){


    var context = {
      resultCursors: result,
    }

    //同时要构造一个相同结构的结果集
    return util.walkAstAsync( ast, function*( astNode, context){
      //console.log('in walk', astNode)
      if( result.nodes[astNode.type] === undefined ) result.nodes[astNode.type] = {}
      trackerNodesCache[astNode.tracker] = []

      //头部处理非常简单
      if( astNode === ast ){
        //TODO 允许 optimizer 接入
        //TODO 允许混合类型的type
        result.trackerRelationMap[astNode.tracker] = {}
        _.forEach(  yield (util.getRootCursor(db, astNode ).toArray()), function( node ){
          var sign = {type:astNode.type, id:node._id}
          result.nodes[astNode.type][node._id] = node
          result.trackerRelationMap[astNode.tracker][node._id] = sign
          trackerNodesCache[astNode.tracker].push(sign)
        })
        return
      }


      //TODO 这是个深度优先遍历，设计一下如何建立相应 result 节点
      //if( context.dive === true && context.lastRelationStrKey ){
      //  //Todo 判断是否是数组
      //  context.resultCursors = context.resultCursors.reduce(function(a, grandNode){
      //    return a.concat( grandNode[context.lastRelationStrKey] )
      //  },[])
      //}

      //根节点数据结构不一样
      result.trackerRelationMap[astNode.tracker] = {
        rawRelation : context.relation,
        parentRelationMap : {}
      }


      for( var i in trackerNodesCache[context.parent.tracker] ){
        var parentSign = trackerNodesCache[context.parent.tracker][i]
        var nodeIds = Object.keys(result.nodes[parentSign.type][parentSign.id][safeMongoKey(context.relationStrKey)] || {} )
        if( nodeIds.length === 0 ){
          console.log( safeMongoKey(context.relationStrKey), 'not in', result.nodes[parentSign.type][parentSign.id])
          continue
        }else{
          console.log( safeMongoKey(context.relationStrKey), nodeIds)
        }

        var nodes =  yield (util.getCurrentCursor( db,astNode, nodeIds).toArray())

        result.trackerRelationMap[astNode.tracker].parentRelationMap[parentSign.id] = {}

        nodes.forEach(function( node ){
          var sign =  {
            type : astNode.type,
            id: node._id
          }
          //覆盖相同的节点，节省数据传输
          result.nodes[astNode.type][node._id] = node
          result.trackerRelationMap[astNode.tracker].parentRelationMap[parentSign.id][node._id] = {
            props : {}, //Todo 获取关联的 props
            target:sign
          }

          trackerNodesCache[astNode.tracker].push( sign )
        })
      }

    },context)


  }).then(function(){
    return result
  })

}



////////////////////////////////////////////////////////////////////////////////
//     push
////////////////////////////////////////////////////////////////////////////////

function *push( ast, rawNodesToSave, trackerRelationMap){
  var root = this
  var types = Object.keys( rawNodesToSave )
  var nodesToSave = {}
  var clientServerIdMap = util.zipObject(types, types.map(function(type){
    nodesToSave[type] = []

    var clientIds = []
    _.forEach(rawNodesToSave[type], function(node, id){
      clientIds.push(id)
      //为了性能，这里把 nodesToSave 也填充了
      nodesToSave[type].push(node)
    })
    return clientIds
  }))

  console.log('begin to push', types)
  log(clientServerIdMap )
  log( nodesToSave)

//TODO 先处理所有要创建的 node


  return  util.connect(database, function(db){

    return co(function *(){

      yield Promise.all(util.map(nodesToSave,function(nodes, type){
        return db.collection(type).insert( nodes ).then(function( savedResult){
          //TODO 这里会不会出现顺序错乱的问题？
          var clientIds = clientServerIdMap[type]
          clientServerIdMap[type] = _.zipObject(clientIds, savedResult.insertedIds.slice(1))
        })
      }))


      //Todo 按照 relation 建立关系
      return yield util.walkAstAsync(ast, function *( astNode, context ){
        if( astNode === ast ) return

        var parentRelationMap= trackerRelationMap[astNode.tracker].parentRelationMap
        yield Promise.all( util.map(parentRelationMap, function( nodeAndProps, parentId){
            //获得保存后的 父id
          if( util.exist(clientServerIdMap,[context.parent.type, parentId] )){
            parentId = clientServerIdMap[context.parent.type][parentId]
          }

          //建立双向连接
          console.log("build bi-relation", parentId, nodeAndProps)
          var toSet = {}
          var safeRelationKey =safeMongoKey(context.relationStrKey)
          //替换所有的 nodeId
          _.forEach(nodeAndProps, function( nodeAndProp , nodeId){
            if( util.exist(clientServerIdMap,[nodeAndProp.target.type, nodeId] )){
              var savedId = clientServerIdMap[nodeAndProp.target.type][nodeId]
              nodeAndProps[savedId] = nodeAndProps[nodeId]
              delete nodeAndProps[nodeId]
            }
          })
          toSet[safeRelationKey] = nodeAndProps

          return db.collection(context.parent.type).update({
            _id : ObjectID( parentId )
          },{
            $set : toSet

          }).then(function(){

            return Promise.all( util.map( nodeAndProps, function(nodeAndProp, nodeId ){



              var reverseToSet = {}
              var reverseSafeRelationKey = reverseRelationKey(safeRelationKey)
              reverseToSet[reverseSafeRelationKey] = {}
              reverseToSet[reverseSafeRelationKey][parentId] = { type : context.parent.type, id:parentId }

              //TODO property 存在哪里？
              console.log('reverse node', nodeId, parentId)

              return db.collection( nodeAndProp.target.type ).update({
                _id : ObjectID( nodeId )
              },{
                $set : reverseToSet
              })
            }))

          })

        }))
      })

    })

  })
}









module.exports = {
  pull,
  destroy,
  push,
}