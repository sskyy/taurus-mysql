'use strict'
//var connection = require('./lib/conneciton')

var RelatedTypes = require('roof-zeroql/lib/RelatedTypes')
var util = require('./lib/util')
var mysql = require('mysql')
var co = require('co')
var _ = require('lodash')


function createConnection(connectionDef) {
  var connection = mysql.createConnection(connectionDef)

  util.promisify(connection, 'query')
  util.promisify(connection, 'beginTransaction')
  util.promisify(connection, 'commit')
  util.promisify(connection, 'connect')
  util.promisify(connection, 'end')

  return connection
}

function makeRelationTableName(relationKey, reverse) {
  return `${reverse ? relationKey.to : relationKey.from}_${relationKey.name}_${reverse ? relationKey.from : relationKey.to}`
}

function stringValue(value) {
  if (typeof value === `string`) {
    return `'${value}'`
  } else if (typeof value === 'object') {
    //TODO 规划可扩展格式
    return `'${JSON.stringify(value)}'`
  } else {
    return value
  }
}

function parseWhereDetail( key, value ){
  if( typeof value === 'object'){
    //TODO operator 只能是 like, < , >, >=, <=, between
    let searchOperator = Object.keys( value ).pop()
    let searchValue = value[searchOperator]
    return `${key} ${searchOperator} ${stringValue(searchValue)}`
  }else{
    return `${key} = ${stringValue(value)}`
  }
}

function parseAstToSqlArgs( ast ){
  var filter = {}
  var where = []
  for (let key in ast.attrs.data) {
    let value = ast.attrs.data[key]
    if (/^_/.test(key)) {
      filter[key] = value
    } else {
      //读取 like 等标记
      //TODO 读取数据的 > < 符号
      if( Object.prototype.toString.call(value) === '[object Array]' ){
        where.push( `(${value.map(v=>parseWhereDetail(key, v)).join(' OR ')})`)

      }else{

        where.push(parseWhereDetail(key, value))
      }
    }
  }


  var fieldsStr = (ast.fields && ast.fields.length) ? ast.fields.join(',') : '*'
  var whereStr = where.length ? `WHERE ${where.join(' AND ')}` : ''
  var limitStr = filter._limit ? `LIMIT ${filter._limit}` : ''
  var offsetStr = filter._offset ? `OFFSET ${filter._offset}` : ''
  var orderByStr = filter._orderBy? `ORDER BY ${filter._orderBy}` : ''

  return {
    fieldsStr,
    whereStr,
    limitStr,
    offsetStr,
    orderByStr,
  }
}


/////////////////////////
//            Taurus
/////////////////////////
function Taurus(connectionDef, types, connection) {
  this.types = new RelatedTypes(types)
  this.connection = connection || createConnection(connectionDef)
}



////////////////////////////////////////////////////////////////////////////////
//     pull
////////////////////////////////////////////////////////////////////////////////
//多个节点
Taurus.prototype.pull = function (ast) {
  var that = this
  //不需要结果树
  var result = {
    nodes: {},
    trackerRelationMap: {}
  }

  var trackerNodesCache = {}

  return co(function *() {

    var context = {
      resultCursors: result,
    }

    //同时要构造一个相同结构的结果集
    //TODO 未来都扔到客户端去构造
    yield util.walkAstAsync(ast, function*(astNode, context) {
      //console.log('in walk', astNode)
      if (result.nodes[astNode.type] === undefined) result.nodes[astNode.type] = {}
      trackerNodesCache[astNode.tracker] = []

      //头部处理非常简单
      if (astNode === ast) {
        //TODO 允许 optimizer 接入
        //TODO 允许混合类型的type
        result.trackerRelationMap[astNode.tracker] = {}
        _.forEach(yield that.getRootNodes(astNode), function (node) {
          var sign = {type: astNode.type, id: node.id}
          result.nodes[astNode.type][node.id] = node
          result.trackerRelationMap[astNode.tracker][node.id] = sign
          trackerNodesCache[astNode.tracker].push(sign)
        })
        return
      }


      //根节点数据结构不一样
      result.trackerRelationMap[astNode.tracker] = {
        rawRelation: context.relation,
        parentRelationMap: {}
      }


      for (var i in trackerNodesCache[context.parent.tracker]) {
        var parentSign = trackerNodesCache[context.parent.tracker][i]
        //开始取当前的 ids
        var nodeIds = (yield that.getRelations( parentSign.id, context.relation, context.relation.reverse))
          .map( relation=> relation[context.relation.reverse?'from':'to'])

        if (nodeIds.length === 0) {
          console.log( context.relation.name, 'not in', result.nodes[parentSign.type][parentSign.id])
          continue
        } else {
          //console.log(safeMongoKey(context.relationStrKey), nodeIds)
        }

        var nodes = yield (that.gerRelatedNodes( astNode, nodeIds))

        result.trackerRelationMap[astNode.tracker].parentRelationMap[parentSign.id] = {}

        nodes.forEach(function (node) {
          var sign = {
            type: astNode.type,
            id: node.id
          }
          //覆盖相同的节点，节省数据传输
          result.nodes[astNode.type][node.id] = node
          result.trackerRelationMap[astNode.tracker].parentRelationMap[parentSign.id][node.id] = {
            props: {}, //Todo 获取关联的 props
            target: sign
          }

          trackerNodesCache[astNode.tracker].push(sign)
        })
      }

    }, context)

    return result
  })

}

Taurus.prototype.getRelations = function( parentId, relationKey, reverse){

  var that = this
  var table = makeRelationTableName(relationKey, reverse)
  return co(function *(){
    return that.connection.query(`SELECT * FROM ${table} WHERE ${reverse?'`to`':'`from`'}=${parentId}`)
  })

}


Taurus.prototype.getRootNodes = function (ast) {
  var that = this
  return co(function*() {
    var args = parseAstToSqlArgs(ast)
    console.log( `SELECT ${args.fieldsStr} FROM ${ast.type} ${args.whereStr} ${args.limitStr} ${args.offsetStr} ${args.orderByStr}`)
    return yield that.connection.query(
      `SELECT ${args.fieldsStr} FROM ${ast.type} ${args.whereStr} ${args.limitStr} ${args.offsetStr} ${args.orderByStr}`
    )
  })
}

Taurus.prototype.gerRelatedNodes = function (ast, nodes) {
  var that = this
  ast.attrs.data.id = nodes
  return co(function*() {
    var args = parseAstToSqlArgs(ast)
    console.log( `SELECT ${args.fieldsStr} FROM ${ast.type} ${args.whereStr} ${args.limitStr} ${args.offsetStr} ${args.orderByStr}`)
    return yield that.connection.query(
      `SELECT ${args.fieldsStr} FROM ${ast.type} ${args.whereStr} ${args.limitStr} ${args.offsetStr} ${args.orderByStr}`
    )
  })
}


//////////////////////////
//                push
//////////////////////////
Taurus.prototype.push = function (ast, rawNodesToSave, trackerRelationMap) {

  var that = this
  var clientServerIdMap

  return co(function *() {
    yield that.connection.beginTransaction()
    clientServerIdMap = yield that.saveClientNodes(rawNodesToSave)
    //开始建立关系
    yield util.walkAstAsync(ast, function *(astNode, context) {
      if (astNode === ast) {
        //console.log("root:", trackerRelationMap[astNode.tracker])
        _.forEach(trackerRelationMap[astNode.tracker], function (node, rawId) {
          if (util.exist(clientServerIdMap, [node.type, rawId])) {
            if (clientServerIdMap[node.type][rawId].trackers === undefined) {
              clientServerIdMap[node.type][rawId].trackers = {}
            }
            clientServerIdMap[node.type][rawId].trackers[astNode.tracker] = true
          }
        })
        return
      }

      var parentRelationMap = trackerRelationMap[astNode.tracker].parentRelationMap

      //console.log('save parent map')

      for (let rawParentId in parentRelationMap) {
        let nodeAndProps = parentRelationMap[rawParentId]
        //获得保存后的 父id
        var parentId
        if (util.exist(clientServerIdMap, [context.parent.type, rawParentId])) {
          parentId = clientServerIdMap[context.parent.type][rawParentId].data.id
          //说明节点也是 cache 被替换了，这里记录一下，方便浏览器端更新 relationMap
          if (clientServerIdMap[context.parent.type][rawParentId].childTrackers === undefined) {
            clientServerIdMap[context.parent.type][rawParentId].childTrackers = {}
          }

          clientServerIdMap[context.parent.type][rawParentId].childTrackers[astNode.tracker] = rawParentId
        } else {
          parentId = rawParentId
        }

        //建立双向连接
        //console.log("build bi-relation", parentId, nodeAndProps)
        //替换所有的 nodeId
        _.forEach(nodeAndProps, function (nodeAndProp, nodeId) {
          if (util.exist(clientServerIdMap, [nodeAndProp.target.type, nodeId])) {
            var savedId = clientServerIdMap[nodeAndProp.target.type][nodeId].data.id
            nodeAndProps[savedId] = nodeAndProps[nodeId]
            delete nodeAndProps[nodeId]

            //这里记录一下 tracker，方便替换 relationMap
            if (clientServerIdMap[nodeAndProp.target.type][nodeId].trackers === undefined) {
              clientServerIdMap[nodeAndProp.target.type][nodeId].trackers = {}
            }
            clientServerIdMap[nodeAndProp.target.type][nodeId].trackers[astNode.tracker] = parentId

          }
        })


        var relationKey = trackerRelationMap[astNode.tracker].relation.key
        yield that.relateChildren({type: context.parent.type, id: parentId}, nodeAndProps, relationKey)
      }
    })

    yield that.connection.commit()

  }).then(function () {
    //TODO return saved nodes
    return {
      clientServerIdMap: clientServerIdMap
    }
  })
}


function makeClientServerIdMap(rawNodesToSave, nodesToSave, nodesToUpdate) {
  return _.mapValues(rawNodesToSave, function (nodes, type) {
    //顺便填充 nodesToSave 和 nodesToUpdate
    nodesToSave[type] = {
      clientIds: [],
      nodes: []
    }
    nodesToUpdate[type] = []

    return _.mapValues(nodes, function (node, indexId) {
      //顺便填充 nodesToSave 和 nodesToUpdate
      if (!node.id && node.id !==0) {
        nodesToSave[type].clientIds.push(indexId)
        nodesToSave[type].nodes.push(node)
      } else {
        nodesToUpdate[type].push(node)
      }

      return {
        data: {}
      }
    })
  })
}


Taurus.prototype.saveClientNodes = function (rawNodesToSave) {
  var that = this
  return co(function *() {
    var nodesToSave = {}
    var nodesToUpdate = {}


    var clientServerIdMap = makeClientServerIdMap(rawNodesToSave, nodesToSave, nodesToUpdate)

    //要创建的数据
    for (let type in nodesToSave) {
      let nodesAndClientIds = nodesToSave[type]
      if (nodesAndClientIds.clientIds.length === 0) continue

      let clientIds = nodesAndClientIds.clientIds
      let nodes = nodesAndClientIds.nodes

      for (let index in nodes) {
        let node = nodes[index]
        let clientId = clientIds[index]
        let fields = util.map(node, function (value, key) {
          return `${key}=${stringValue(value)}`
        }).join(',')
        //console.log('query', `INSERT INTO ${type} SET ${fields}`)
        var result = yield that.connection.query(`INSERT INTO ${type} SET ${fields}`)

        clientServerIdMap[type][clientId] = {data: {id: result.insertId}}
      }
    }

    //要更新的数据
    for (let type in nodesToUpdate) {
      let nodes = nodesToUpdate[type]
      if (nodes.length === 0) continue


      for (let index in nodes) {
        let node = nodes[index]
        var toUpdateData = _.cloneDeep(node)
        var _id = node.id
        delete toUpdateData.id

        let fields = util.map(toUpdateData, function (value, key) {
          return `${key}=${stringValue(value)}`
        }).join(',')

        console.log(`UPDATE ${type} SET ${fields} WHERE id=${_id}`)
        yield that.connection.query(`UPDATE ${type} SET ${fields} WHERE id=${_id}`)

        clientServerIdMap[type][_id] = {
          data: _.extend(toUpdateData, {
            id: _id
          })
        }
      }
    }


    return clientServerIdMap
  })
}


Taurus.prototype.relateChildren = function (parent, nodeAndProps, relationKey) {
  //console.log('relating', parent, nodeAndProps)
  var relationTable = makeRelationTableName(relationKey)
  var that = this
  return co(function *() {
    for (let nodeId in nodeAndProps) {
      let props = stringValue(nodeAndProps[nodeId].props)
      //TODO property 存在哪里？
      //console.log('node and prop', nodeAndProp)
      var reverse = relationKey.to === parent.type
      var values = reverse ? [nodeId, parent.id] : [parent.id, nodeId]
      values.push( props)
      console.log(`INSERT INTO ${relationTable} (\`from\`,\`to\`, props) VALUES (${values.join(',')})`)
      yield that.connection.query(`INSERT INTO ${relationTable} (\`from\`,\`to\`, props) VALUES (${values.join(',')})`)
    }
  })
}


////////////////////////////
//               destroy
////////////////////////////
Taurus.prototype.destroy = function (type, id) {
  //console.log("tring to destroy", type, id)
  var that = this
  return co(function *(){
    yield that.connection.beginTransaction()
    yield that.connection.query(`DELETE FROM ${type} WHERE id = ${id}`)
    var relations = that.types.getRelations(type)
    //console.log('relations found====>', relations)
    for( let index in relations ){
      let relation = relations[index]
      var tableName = makeRelationTableName(relation.key)
      var indexKey = relation.key.from === type ? '`from`' : '`to`'
      //console.log(`deleting relation DELETE FROM ${tableName} WHERE ${indexKey}=${id}`)
      yield that.connection.query(`DELETE FROM ${tableName} WHERE ${indexKey}=${id}`)

    }
    yield that.connection.commit()
  })

}

////////////////////////////
//            connect & end
///////////////////////////
Taurus.prototype.connect = function *() {
  return yield this.connection.connect()
}

Taurus.prototype.end = function *() {
  return yield this.connection.end()
}

module.exports = Taurus