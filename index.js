'use strict'

var RelatedTypes = require('roof-zeroql/lib/RelatedTypes')
var util = require('./lib/util')
var mysql = require('mysql')
var co = require('co')
var _ = require('lodash')

function print(obj) {
  console.log(JSON.stringify(obj, null, 4))
}
var log = console.log.bind(console)

function createConnection(connectionDef) {
  var connection = mysql.createConnection(connectionDef)

  util.promisify(connection, 'query')
  util.promisify(connection, 'beginTransaction')
  util.promisify(connection, 'commit')
  util.promisify(connection, 'connect')
  util.promisify(connection, 'end')

  return connection
}

function makeRelationTableName(relationKey) {
  return `${relationKey.reverse ? relationKey.to : relationKey.from}_${relationKey.name}_${relationKey.reverse ? relationKey.from : relationKey.to}`
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

function parseWhereDetail(key, value) {
  if (typeof value === 'object') {
    //TODO operator 只能是 like, < , >, >=, <=, between
    let searchOperator = Object.keys(value).pop()
    let searchValue = value[searchOperator]
    return `${key} ${searchOperator} ${stringValue(searchValue)}`
  } else {
    return `${key} = ${stringValue(value)}`
  }
}

function parseAstToSqlArgs(ast) {
  //console.log('parsing')
  //print(ast)
  var filter = {}
  var where = []
  for (let key in ast.attrs.data) {
    let value = ast.attrs.data[key]
    if (/^_/.test(key)) {
      filter[key] = value
    } else {
      if (Object.prototype.toString.call(value) === '[object Array]' ) {
        //TODO 读取数据的 > < 符号
        if(value.length !== 0) where.push(`(${value.map(v=>parseWhereDetail(key, v)).join(' OR ')})`)
      } else {
        where.push(parseWhereDetail(key, value))
      }
    }
  }

  //TODO 强制带上 id 的逻辑 放到别的地方更合理。
  if( ast.fields && ast.fields.indexOf('id') === -1 ) ast.fields.push('id')

  var fieldsStr = (ast.fields && ast.fields.length) ? ast.fields.join(',') : '*'
  var whereStr = where.length ? `WHERE ${where.join(' AND ')}` : ''
  var orderByStr = filter._orderBy ? `ORDER BY ${filter._orderBy}` : ''
  var groupByStr = filter._groupBy ? `GROUP BY ${filter._groupBy}` : ''
  var limitStr = ''
  if( filter._limit ){
    limitStr = filter._offset ? `LIMIT ${filter._offset},${filter._limit}` :`LIMIT ${filter._limit}`
  }

  return {
    fieldsStr,
    whereStr,
    limitStr,
    orderByStr,
    groupByStr,
    total: filter._total !== undefined,
    limit : filter._limit,
    offset : filter._offset
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
    ast: _.cloneDeep(ast)
  }


  return co(function *() {


    //同时要构造一个相同结构的结果集
    //TODO 未来都扔到客户端去构造
    yield util.walkAstAsync(result.ast, function*(astNode, context) {
      util.ensure(result.nodes, astNode.type, {})

      //头部处理非常简单
      if (astNode === result.ast) {
        //TODO 允许 optimizer 接入
        //TODO 允许混合类型的type
        let queryResult = yield that.getRootNodes(astNode)
        astNode.data = {
          nodes: _.map(queryResult.nodes,node=> {
            result.nodes[astNode.type][node.id] = node
            //ast 上只要存 sign 就够了
            return {type: astNode.type, id: node.id}
          }),
          total: queryResult.total,
          limit : queryResult.limit,
          offset: queryResult.offset
        }


        return
      }

      //普通节点
      var parentIds = []
      if (context.parent === result.ast) {
        //如果父节点就是根
        parentIds = context.parent.data.nodes.map(node=>node.id)
        log("parentid from root", parentIds)
      } else {
        /*
         data:{
         parentId : {
         nodes : {},
         total : 10,
         }
         */
        parentIds = _.reduce(context.parent.data, (result, nodesData)=> {
          return result.concat(Object.keys(nodesData.nodes))
        }, [])
        log('parentIds', parentIds)
      }

      if( parentIds.length !== 0 ){
        let queryResult =yield that.gerRelatedNodes(astNode, parentIds, context.relation)
        console.log("=>>>>>astNode.type")
        print( queryResult)
        astNode.data = _.mapValues( queryResult, nodeData=>{
          //ast 上只要存 sign 就够了

          return {
            nodes:_.mapValues(nodeData.nodes, node=>{
              result.nodes[astNode.type][node.id] = node
              return {type: astNode.type, id: node.id}
            }),
            total:nodeData.total,
            limit : nodeData.limit
          }
        })
      }
    })

    log('pull result')
    print(result)
    return result
  })

}

Taurus.prototype.getRelations = function (parentId, relationKey, reverse) {

  var that = this
  var table = makeRelationTableName(relationKey, reverse)
  return co(function *() {
    return that.connection.query(`SELECT * FROM ${table} WHERE ${reverse ? '`to`' : '`from`'}=${parentId}`)
  })
}

Taurus.prototype.getRelatedNodeIds = function (sourceIds, relation) {

  var table = makeRelationTableName(relation)
  var that = this
  return co(function *() {
    var results = {}

    sourceIds.forEach(sourceId=> {
      results[sourceId] = []
    })
    console.log('getting node ids', sourceIds)
    //TODO sourceIDs 出问题了
    log(`SELECT * FROM ${table} WHERE ${relation.reverse ? '`to`' : '`from`'} IN (${sourceIds.map(id=>stringValue(id)).join(',')})`)
    let queryResult = yield that.connection.query(`SELECT * FROM ${table} WHERE ${relation.reverse ? '`to`' : '`from`'} IN (${sourceIds.map(id=>stringValue(id)).join(',')})`)

    queryResult.forEach(record=> {
      var sourceKey = relation.reverse ? 'to' : 'from'
      var targetKey = relation.reverse ? 'from' : 'to'
      results[record[sourceKey]].push(record[targetKey])
    })
    return results
  })
}


Taurus.prototype.getRootNodes = function (ast) {
  var that = this
  return co(function*() {
    var args = parseAstToSqlArgs(ast)
    console.log(`SELECT ${args.fieldsStr} FROM ${ast.type} ${args.whereStr} ${args.orderByStr} ${args.limitStr} `)
    var result = {
      nodes: yield that.connection.query(
        `SELECT ${args.fieldsStr} FROM ${ast.type} ${args.whereStr}  ${args.orderByStr} ${args.groupByStr} ${args.limitStr} `
      )
    }

    if (args.total) {
      result.total = (yield that.connection.query(
        `SELECT count(*) AS count FROM ${ast.type} ${args.whereStr}`
      ))[0].count
    }

    if( args.limit ){
      result.limit = args.limit
    }

    result.offset = args.offset || 0

    return result

  })
}

Taurus.prototype.gerRelatedNodes = function (ast, parentIds, relation) {
  var that = this

  console.log('getting related nodes from parentIds', parentIds)

  return co(function*() {
    var candidateRelatedIdsMap = yield that.getRelatedNodeIds(parentIds, relation)
    var resultMap = _.mapValues(candidateRelatedIdsMap, nodes=> {
      return {nodes: nodes}
    })

    //看看是一次读出来，还是分多次读

    if (ast.attrs.data._total) {
      //必须分多次读
      for (let parentId in resultMap) {
        ast.attrs.data.id = Object.keys(resultMap[parentId].nodes)
        let args = parseAstToSqlArgs(ast)
        console.log( `==>SELECT ${args.fieldsStr} FROM ${ast.type} ${args.whereStr} ${args.orderByStr} ${args.groupByStr} ${args.limitStr} `)
        let nodeRecords = yield that.connection.query(
          `SELECT ${args.fieldsStr} FROM ${ast.type} ${args.whereStr} ${args.orderByStr} ${args.groupByStr} ${args.limitStr} `
        )
        console.log('query count', `SELECT count(*) AS count FROM ${ast.type} ${args.whereStr} `)
        let total = (yield that.connection.query(
          `SELECT count(*) AS count FROM ${ast.type} ${args.whereStr} `
        ))[0].count

        resultMap[parentId].nodes = _.indexBy(nodeRecords,'id')
        resultMap[parentId].total = total
        resultMap[parentId].limit = args.limit
        resultMap[parentId].offset= args.offset || 0
      }



    } else {
      //可以一次全读出来
      ast.attrs.data.id = _.reduce(candidateRelatedIdsMap, (resultIds, ids)=> {
        return resultIds.concat(ids)
      }, [])
      let args = parseAstToSqlArgs(ast)
      console.log(`SELECT ${args.fieldsStr} FROM ${ast.type} ${args.whereStr} ${args.orderByStr} ${args.limitStr} `)
      let nodeRecords = _.indexBy(yield that.connection.query(
        `SELECT ${args.fieldsStr} FROM ${ast.type} ${args.whereStr} ${args.orderByStr} ${args.groupByStr} ${args.limitStr} `
      ), 'id')

      _.forEach(resultMap, nodeResult=> {
        //不记录 total 信息
        nodeResult.total = null
        nodeResult.nodes = _.indexBy(_.compact(nodeResult.nodes.map(nodeId=>nodeRecords[nodeId])),'id')
        nodeResult.limit = args.limit
        nodeResult.offset = args.offset || 0
      })
    }

    return resultMap
  })
}


//////////////////////////
//                push
//////////////////////////
Taurus.prototype.push = function (ast, rawNodesToSave, relationAst) {
  console.log('pushing')
  print(relationAst)
  print(rawNodesToSave)
  var that = this
  var clientServerIdMap

  return co(function *() {
    yield that.connection.beginTransaction()
    clientServerIdMap = yield that.saveClientNodes(rawNodesToSave)
    //开始建立关系
    yield util.walkAstAsync(relationAst, function *(astNode, context) {
      if (astNode === relationAst) {
        //console.log("root:", trackerRelationMap[astNode.tracker])
        _.forEach(astNode.data.nodes, function (sign, rawId) {
          if (util.exist(clientServerIdMap, [sign.type, rawId])) {
            if (clientServerIdMap[sign.type][rawId].trackers === undefined) {
              clientServerIdMap[sign.type][rawId].trackers = {}
            }
            clientServerIdMap[sign.type][rawId].trackers[astNode.tracker] = true
          }
        })
        return
      }

      //普通节点
      for (let rawParentId in astNode.data) {
        let nodesData = astNode.data[rawParentId]
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
        _.forEach(nodesData.nodes, function (signAndProps, nodeId) {
          if (util.exist(clientServerIdMap, [astNode.type, nodeId])) {
            var savedId = clientServerIdMap[astNode.type][nodeId].data.id
            nodesData.nodes[savedId] = nodesData.nodes[nodeId]
            delete nodesData.nodes[nodeId]

            //这里记录一下 tracker，方便替换 relationMap
            if (clientServerIdMap[astNode.type][nodeId].trackers === undefined) {
              clientServerIdMap[astNode.type][nodeId].trackers = {}
            }
            clientServerIdMap[astNode.type][nodeId].trackers[astNode.tracker] = parentId

          }
        })

        //TODO 最好从 RelatedTypes 里面取
        let relation = {
          to : astNode.type,
          from : context.parent.type,
          name : context.relation.name,
          reverse : context.relation.reverse
        }
        yield that.relateChildren(relation, parentId, nodesData.nodes)
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
      if (!node.id && node.id !== 0) {
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


Taurus.prototype.relateChildren = function (relation, parentId,  nodeAndProps) {
  //console.log('relating', parent, nodeAndProps)
  var relationTable = makeRelationTableName(relation)
  var that = this
  return co(function *() {
    for (let nodeId in nodeAndProps) {
      let props = stringValue(nodeAndProps[nodeId].props || {})
      //TODO property 存在哪里？
      //console.log('node and prop', nodeAndProp)
      var values = relation.reverse ? [nodeId, parentId] : [parentId, nodeId]
      values.push(props)
      console.log(`INSERT INTO ${relationTable} (\`from\`,\`to\`, props) VALUES (${values.join(',')})`)
      yield that.connection.query(`INSERT INTO ${relationTable} (\`from\`,\`to\`, props) VALUES (${values.join(',')})`)
    }
  })
}


///////////////////////////
//              update
///////////////////////////
//TODO 批量更新


////////////////////////////
//               destroy
////////////////////////////
Taurus.prototype.destroy = function (type, id) {
  //console.log("tring to destroy", type, id)
  var that = this
  return co(function *() {
    yield that.connection.beginTransaction()
    yield that.connection.query(`DELETE FROM ${type} WHERE id = ${id}`)
    var relations = that.types.getRelations(type)
    //console.log('relations found====>', relations)
    for (let index in relations) {
      let relation = relations[index]
      var tableName = makeRelationTableName(relation.key)
      var indexKey = relation.key.from === type ? '`from`' : '`to`'
      //console.log(`deleting relation DELETE FROM ${tableName} WHERE ${indexKey}=${id}`)
      yield that.connection.query(`DELETE FROM ${tableName} WHERE ${indexKey}=${id}`)

    }
    yield that.connection.commit()
  })

}

//TODO 批量销毁

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