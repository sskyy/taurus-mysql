var _ = require('lodash')
var util = require('./util')
var mongodb = require('mongodb')
var ObjectID = mongodb.ObjectID
var log = require('pretty-log-2').pp
var co = require('co')
var print = require('pretty-log-2').pp


var RelatedTypes = require('roof-zero/lib/RelatedTypes')
var mysql = require('mysql')
var connection = mysql.createConnection({
  host     : 'example.org',
  user     : 'bob',
  password : 'secret'
});

connection.query = util.promisify(connection, 'query')
connection.beginTransaction = util.promisify(connection, 'beginTransaction')
connection.commit = util.promisify(connection, 'commit')
connection.connect = util.promisify(connection, 'connect')
connection.end = util.promisify(connection, 'end')

var relatedTypes = new RelatedTypes



////////////////////////////////////////////////////////////////////////////////
//     destroy
////////////////////////////////////////////////////////////////////////////////
//单个节点
//TODO 改善性能
function *destroy( database, type,id ){
  console.log( "tring to destroy",type, id)



  yield connection.beginTransaction()
  yield connection.query(`DELETE FROM ${type} WHERE id = ${id}`)
  var relations = relatedTypes.getRelations(type)
  yield Promise.all(relations.map((relation)=>{
    var tableName = makeRelationTableName( relation )
    var indexKey = relation.from === type ? 'from':'to'
    return connection.query(`DELETE FROM ${tableName} WHERE ${indexKey}=${id}`)
  })

  yield connection.commit()
  return true

  //////////////////////
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
}

////////////////////////////////////////////////////////////////////////////////
//     pull
////////////////////////////////////////////////////////////////////////////////
//多个节点
function *pull( database, ast ){

  //不需要结果树

  var result = {
    nodes : {},
    trackerRelationMap : {
    }
  }

  var trackerNodesCache = {}


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
          node._id = node._id.toHexString()
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
          node._id = node._id.toHexString()
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
function relateChildren( db, parent, nodeAndProps, relationStrKey ){
  var toSet = {}
  var safeRelationKey =safeMongoKey(relationStrKey)
  toSet[safeRelationKey] = nodeAndProps


  return db.collection(parent.type).update({
    _id : ObjectID( parent.id )
  },{
    $set : toSet
  }).then(function(){

    return Promise.all( util.map( nodeAndProps, function(nodeAndProp, nodeId ){

      var reverseToSet = {}
      var reverseSafeRelationKey = reverseRelationKey(safeRelationKey)
      reverseToSet[reverseSafeRelationKey] = {}
      reverseToSet[reverseSafeRelationKey][parent.id] = { type : parent.type, id:parent.id }

      //TODO property 存在哪里？
      console.log('reverse node', nodeId, parent.id)

      return db.collection( nodeAndProp.target.type ).update({
        _id : ObjectID( nodeId )
      },{
        $set : reverseToSet
      })
    }))

  })
}



function saveClientNodes(db, rawNodesToSave ){
  return co(function *(){
    var nodesToSave = {}
    var nodesToUpdate = {}


    var clientServerIdMap = _.mapValues( rawNodesToSave, function( nodes, type){
      //顺便填充 nodesToSave 和 nodesToUpdate
      nodesToSave[type] = {
        clientIds : [],
        nodes : []
      }
      nodesToUpdate[type] = []

      return _.mapValues( nodes, function(node, indexId){
        //顺便填充 nodesToSave 和 nodesToUpdate
        if( node._id === undefined ){
          nodesToSave[type].clientIds.push( indexId )
          nodesToSave[type].nodes.push( node )
        }else{
          nodesToUpdate[type].push( node )
        }

        return {
          data : {}
        }
      })
    })


    //要创建的数据
    yield  util.map(nodesToSave,function(nodesAndClientIds, type){
      if( nodesAndClientIds.clientIds.length === 0 ) return

      var clientIds = nodesAndClientIds.clientIds
      var nodes = nodesAndClientIds.nodes

      return db.collection(type).insert( nodes ).then(function( savedResult){
        //TODO 这里会不会出现顺序错乱的问题？
        var indexedMap = _.zipObject(clientIds,
          savedResult.insertedIds.slice(1).map(function(_id){
            return {
              //TODO 数据要不要完整传回？
              //TODO 这里默认只返回了 _id，应该根据primary返回
              data : {
                _id: _id.toHexString()
              }
            }
          }))

        _.extend( clientServerIdMap[type], indexedMap)
      })
    })

    //要更新的数据
    yield  util.map(nodesToUpdate,function(nodes, type){
      if( nodes.length === 0 ) return

      return Promise.all( nodes.map(function( node ){
        var toUpdateData = _.cloneDeep( node )
        var _id = node._id
        delete toUpdateData._id

        return db.collection(type).findOneAndUpdate( {_id:ObjectID(_id)} ,{
          $set : toUpdateData
        }).then(function(res){
          print(res)
          //TODO 这里会不会出现顺序错乱的问题？
          clientServerIdMap[type][_id] = {
            data : _.extend(toUpdateData, {
              _id : _id
            })
          }
        })
      }))

    })

    return clientServerIdMap
  })
}



function push( database, ast, rawNodesToSave, trackerRelationMap){
  var root = this
  var types = Object.keys( rawNodesToSave )


  console.log('begin to push', types)

  return  util.connect(database, function *(db){

      //先处理所有要创建的 node
      var clientServerIdMap = yield saveClientNodes(db, rawNodesToSave)

      //按照 relation 建立关系
      yield util.walkAstAsync(ast, function *( astNode, context ) {
        if (astNode === ast){
          console.log("root:", trackerRelationMap[astNode.tracker] )
          _.forEach( trackerRelationMap[astNode.tracker], function(node, rawId){
            if (util.exist(clientServerIdMap, [node.type, rawId])) {
              if( clientServerIdMap[node.type][rawId].trackers === undefined ){
                clientServerIdMap[node.type][rawId].trackers = {}
              }
              clientServerIdMap[node.type][rawId].trackers[astNode.tracker] = true
            }
          })
          return
        }

        var parentRelationMap = trackerRelationMap[astNode.tracker].parentRelationMap
        yield util.map(parentRelationMap, function (nodeAndProps, rawParentId) {
          //获得保存后的 父id
          var parentId
          if (util.exist(clientServerIdMap, [context.parent.type, rawParentId])) {
            parentId = clientServerIdMap[context.parent.type][rawParentId].data._id
            //说明节点也是 cache 被替换了，这里记录一下，方便浏览器端更新 relationMap
            if( clientServerIdMap[context.parent.type][rawParentId].childTrackers === undefined ){
              clientServerIdMap[context.parent.type][rawParentId].childTrackers = {}
            }

            clientServerIdMap[context.parent.type][rawParentId].childTrackers[astNode.tracker] = rawParentId
          }else{
            parentId = rawParentId
          }

          //建立双向连接
          console.log("build bi-relation", parentId, nodeAndProps)
          //替换所有的 nodeId
          _.forEach(nodeAndProps, function (nodeAndProp, nodeId) {
            if (util.exist(clientServerIdMap, [nodeAndProp.target.type, nodeId])) {
              var savedId = clientServerIdMap[nodeAndProp.target.type][nodeId].data._id
              nodeAndProps[savedId] = nodeAndProps[nodeId]
              delete nodeAndProps[nodeId]

              //这里记录一下 tracker，方便替换 relationMap
              if(clientServerIdMap[nodeAndProp.target.type][nodeId].trackers === undefined ){
                clientServerIdMap[nodeAndProp.target.type][nodeId].trackers= {}
              }
              clientServerIdMap[nodeAndProp.target.type][nodeId].trackers[astNode.tracker] = parentId

            }
          })

          return relateChildren(db, {type: context.parent.type, id: parentId}, nodeAndProps, context.relationStrKey)
        })
      })

    //TODO return saved nodes
    return {
      clientServerIdMap : clientServerIdMap
    }

  })
}




module.exports = {
  connect : connection.connect,
  end:connection.end,
  types : types,
  pull,
  destroy,
  push,
}