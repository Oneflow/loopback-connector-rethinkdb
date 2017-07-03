/* jshint sub: true */
var r = require('rethinkdb');
var moment = require('moment');
var async = require('async');
var _ = require('lodash');
var util = require('util');
var Rx = require('rx');

var Connector = require('loopback-connector').Connector;

exports.initialize = function initializeDataSource(dataSource, callback) {
  if (!r) return;

  var s = dataSource.settings;

  if (dataSource.settings.rs) {
    s.rs = dataSource.settings.rs;
    if (dataSource.settings.url) {
      var uris = dataSource.settings.url.split(',');
      s.hosts = [];
      s.ports = [];
      uris.forEach(function (uri) {
        var url = require('url').parse(uri);

        s.hosts.push(url.hostname || 'localhost');
        s.ports.push(parseInt(url.port || '28015', 10));

        if (!s.database) s.database = url.pathname.replace(/^\//, '');
        if (!s.username) s.username = url.auth && url.auth.split(':')[0];
        if (!s.password) s.password = url.auth && url.auth.split(':')[1];
      });
    }

    s.database = s.database || 'test';
  } else {
    if (dataSource.settings.url) {
      var url = require('url').parse(dataSource.settings.url);
      s.host = url.hostname;
      s.port = url.port;
      s.database = (url.pathname || '').replace(/^\//, '');
      s.username = url.auth && url.auth.split(':')[0];
      s.password = url.auth && url.auth.split(':')[1];
    }

    s.host = s.host || 'localhost';
    s.port = parseInt(s.port || '28015', 10);
    s.database = s.database || 'test';
  }

  s.safe = s.safe || false;

  dataSource.adapter = new RethinkDB(s, dataSource, r);
  dataSource.connector = dataSource.adapter;

  if (callback) {
    dataSource.connector.connect(callback);
  }

  process.nextTick(callback);
};

function RethinkDB(s, dataSource, r) {
  Connector.call(this, 'rethink', s);
  this.dataSource = dataSource;
  this.database = s.database;
  this.r = r;
}

util.inherits(RethinkDB, Connector);

RethinkDB.prototype.connect = function (cb) {
  var _this = this;
  var s = _this.settings;
  if (_this.db) {
    process.nextTick(function () {
      cb && cb(null, _this.db);
    });
  } else {
    var cOpts = Object.assign({
      host: s.host,
      port: s.port,
      authKey: s.password,
    }, s.additionalSettings);
    if (cOpts.ssl && cOpts.ssl.ca) {
      // check if is a base64 encoded string
      if (/^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{4}|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)$/.test(cOpts.ssl.ca)) {
        cOpts.ssl.ca = Buffer.from(cOpts.ssl.ca, 'base64');
      }
    }
    r.connect(cOpts, function (error, client) {
      _this.db = client;
      cb && cb(error, client);
    });
  }
};

RethinkDB.prototype.getTypes = function () {
  return ['db', 'nosql', 'rethinkdb'];
};

RethinkDB.prototype.getDefaultIdType = function () {
  return String;
};

RethinkDB.prototype.tableName = function (model) {
  var modelClass = this._models[model];
  if (modelClass.settings.rethinkdb) {
    model = _.get(modelClass, 'settings.rethinkdb.collection') || modelClass.settings.plural || model;
  }
  return model;
};

// Override define model function
RethinkDB.prototype.define = function (modelDefinition) {
  modelDefinition.settings = modelDefinition.settings || {};
  this._models[modelDefinition.model.modelName] = modelDefinition;
  // this.autoupdate(modelDefinition.model.modelName,function(){})
};


RethinkDB.prototype.autoupdate = function (models, cb) {
  var _this = this;
  if (_this.db) {
    if (_this.debug) {
      debug('autoupdate');
    }
    if ((!cb) && ('function' === typeof models)) {
      cb = models;
      models = undefined;
    }
    // First argument is a model name
    if ('string' === typeof models) {
      models = [models];
    }

    models = models || Object.keys(_this._models);

    var enableGeoIndexing = this.settings.enableGeoIndexing === true;

    async.each(models, function (model, modelCallback) {
      var indexes = _this._models[model].settings.indexes || [];
      var indexList = [];
      var index = {};
      var options = {};

      if (typeof indexes === 'object') {
        for (var indexName in indexes) {
          index = indexes[indexName];
          if (index.keys) {
            // The index object has keys
            options = index.options || {};
            index.options = options;
          } else {
            index = {
              keys: index,
              options: {},
            };
          }
          index.name = indexName;
          indexList.push(index);
        }
      } else if (Array.isArray(indexes)) {
        indexList = indexList.concat(indexes);
      }

      if (_this.debug) {
        debug('create indexes: ', indexList);
      }

      r.db(_this.database).table(_this.tableName(model)).indexList().run(_this.db, function (error, cursor) {
        if (error) return cb(error);

        cursor.toArray(function (error, alreadyPresentIndexes) {
          if (error) return cb(error);

          async.each(indexList, function (index, indexCallback) {
            if (_this.debug) {
              debug('createIndex: ', index);
            }

            if (alreadyPresentIndexes.includes(index.name)) {
              return indexCallback();
            }

            var query = r.db(_this.database).table(_this.tableName(model));
            var keys = Object.keys(index.fields || index.keys).map(function (key) {
              return _this.getRow(model, key);
            });
            query = query.indexCreate(index.name, keys.length === 1 ? keys[0] : keys, index.options);
            query.run(_this.db, indexCallback);
            
          }, modelCallback);

        });
      });
    }, cb);
  } else {
    _this.dataSource.once('connected', function () {
      _this.autoupdate(models, cb);
    });
  }
};

// drops tables and re-creates them
RethinkDB.prototype.automigrate = _.debounce(function (models, done) {
  var _this = this;
  var client = this.db;

  if (!client) {
    _this.dataSource.once('connected', function () {
      _this.automigrate(models, done);
    });
    return;
  }

  if ((!done) && ('function' === typeof models)) {
    done = models;
    models = undefined;
  }

  // First argument is a model name
  if ('string' === typeof models) {
    models = [models];
  }

  models = _.uniq(models || Object.keys(_this._models));

  r.db(_this.database).tableList().run(client, function (error, cursor) {
    if (error) {
      return done(error);
    }
    var migratedTables = [];
    cursor.toArray(function (error, list) {
      async.eachSeries(models, function (model, cb) {
        const tableName = _this.tableName(model);
        if (migratedTables.indexOf(tableName) === -1) {
          migratedTables.push(tableName);
          r.db(_this.database).tableDrop(tableName).run(client, function (error) {
            r.db(_this.database).tableCreate(tableName).run(client, function (error) {
              if (error) return cb(error);
              cb();
            });
          });
        } else {
          cb();
        }
      }, function (err) {
        if (err) return done(err);
        _this.autoupdate(models, done);
      });
    });
  });
}, 4000);

// checks if database needs to be actualized
RethinkDB.prototype.isActual = function (cb) {
  var _this = this;
  var client = this.db;

  r.db(_this.database).tableList().run(client, function (error, cursor) {
    if (error) return cb(error);
    if (!cursor.next()) return cb(null, _.isEmpty(_this._models));

    cursor.toArray(function (error, list) {
      if (error) {
        return cb(error);
      }
      var actual = true;
      async.each(Object.keys(_this._models), function (model, cb2) {
        if (!actual) return cb2();

        var properties = _this._models[model].properties;
        var settings = _this._models[model].settings;
        var indexCollection = _.extend({}, properties, settings);
        if (list.indexOf(model) < 0) {
          actual = false;
          cb2();
        } else {
          r.db(_this.database).table(_this.tableName(model)).indexList().run(client, function (error, cursor) {
            if (error) return cb2(error);

            cursor.toArray(function (error, list) {
              if (error || !actual) return cb2(error);

              Object.keys(indexCollection).forEach(function (property) {
                if (_hasIndex(_this, model, property) && list.indexOf(property) < 0)
                  actual = false;
              });
              cb2();
            });
          });
        }
      }, function (err) {
        cb(err, actual);
      });
    });
  });
};

RethinkDB.prototype.create = function (model, data, callback) {
  var idValue = this.getIdValue(model, data);
  var idName = this.idName(model);

  if (idValue === null || idValue === undefined) {
    delete data[idName];
  } else {
    data.id = idValue; // Set it to _id
    idName !== 'id' && delete data[idName];
  }

  if (process.env.TESTING) {
    data._createdAt = new Date();
  }

  this.save(model, data, callback, true);
};

RethinkDB.prototype.updateOrCreate = function (model, data, callback) {
  var idValue = this.getIdValue(model, data);
  var idName = this.idName(model);

  if (idValue === null || idValue === undefined) {
    delete data[idName];
  } else {
    data.id = idValue; // Set it to _id
    idName !== 'id' && delete data[idName];
  }

  this.save(model, data, callback, false, true);
};

RethinkDB.prototype.save = function (model, data, callback, strict, returnObject) {
  var _this = this;
  var client = this.db;

  if (!client) {
    _this.dataSource.once('connected', function () {
      _this.save(model, data, callback, strict, returnObject);
    });
    return;
  }

  var idValue = _this.getIdValue(model, data);
  var idName = _this.idName(model);

  if (strict == undefined)
    strict = false;

  Object.keys(data).forEach(function (key) {
    if (data[key] === undefined)
      data[key] = null;
  });

  r.db(_this.database).table(_this.tableName(model)).insert(data, {
    conflict: strict ? 'error' : 'update',
    returnChanges: true,
  }).run(client, function (err, m) {
    err = err || m.first_error && new Error(m.first_error);
    if (err) {
      callback && callback(err);
    } else {
      var info = {};
      var object = null;

      if (m.inserted > 0) {
        // create
        info.isNewInstance = true;
        idValue = m.changes[0].new_val.id;
      }
      if (m.changes && m.changes.length > 0) {
        // update
        object = m.changes[0].new_val;
        _this.setIdValue(model, object, idValue);
        idName !== 'id' && delete object._id;
      }

      if (returnObject && m.changes && m.changes.length > 0) {
        callback && callback(null, object, info);
      } else {
        callback && callback(null, idValue, info);
      }
    }
  });
};

RethinkDB.prototype.exists = function (model, id, callback) {
  var _this = this;
  var client = this.db;

  if (!client) {
    _this.dataSource.once('connected', function () {
      _this.exists(model, id, callback);
    });
    return;
  }

  r.db(_this.database).table(_this.tableName(model)).get(id).run(client, function (err, data) {
    callback(err, !!(!err && data));
  });
};

RethinkDB.prototype.find = function find(model, id, options, callback) {
  var _this = this,
      _keys;
  var client = this.db;

  if (!client) {
    _this.dataSource.once('connected', function () {
      _this.find(model, id, callback);
    });
    return;
  }

  var idName = this.idName(model);

  var promise = r.db(_this.database).table(_this.tableName(model));

  if (idName == 'id')
    promise = promise.get(id);
  else
    promise = promise.filter({ idName: id });

  var rQuery = promise.toString();

  promise.run(client, function (error, data) {
    // Acquire the keys for this model
    _keys = _this._models[model].properties;

    if (data) {
      // Pass to expansion helper
      _expandResult(data, _keys);
    }

    // Done
    callback && callback(error, data, rQuery);
  });
};

RethinkDB.prototype.destroy = function destroy(model, id, callback) {
  var _this = this;
  var client = this.db;

  if (!client) {
    _this.dataSource.once('connected', function () {
      _this.destroy(model, id, callback);
    });
    return;
  }

  r.db(_this.database).table(_this.tableName(model)).get(id).delete().run(client, function (error, result) {
    callback(error);
  });
};

RethinkDB.prototype.changeFeed = function (model, filter, options) {
  var _this = this;
  var client = this.db;

  if (!client) {
    _this.dataSource.once('connected', function () {
      _this.changeFeed(model, filter, options);
    });
    return;
  }

  if (!filter) {
    filter = {};
  }

  var promise = r.db(_this.database).table(_this.tableName(model));

  var idName = this.idName(model);

  if (filter.order) {
    var keys = filter.order;
    if (typeof keys === 'string') {
      keys = keys.split(',');
    }
    keys.forEach(function (key) {
      var m = key.match(/\s+(A|DE)SC$/);
      key = key.replace(/\s+(A|DE)SC$/, '').trim();
      if (m && m[1] === 'DE') {
        promise = promise.orderBy({ index: r.desc(key) });
      } else {
        promise = promise.orderBy({ index: r.asc(key) });
      }
    });
  } else {
    // default sort by id
    promise = promise.orderBy({ 'index': r.asc('id') });
  }

  if (filter.where) {
    if (filter.where[idName]) {
      var id = filter.where[idName];
      delete filter.where[idName];
      filter.where.id = id;
    }
    promise = this.buildWhere(model, filter.where, promise);
    /* if (promise === null)
     return callback && callback(null, []) */
  }

  if (!_.isEmpty(filter.fields)) {
    promise = _this.buildPluck(model, filter.fields, promise);
  }
  
  if (filter.limit) {
    promise = promise.limit(filter.limit);
  }

  var defaultOptions = {
    changesOptions: {
      include_states: true,
    },
  };

  var feedOptions = _.defaultsDeep({}, defaultOptions, options);

  var rQuery = promise.toString();

  // console.log(rQuery)

  var observable = Rx.Observable.create(function (observer) {
    var sendResults = function () {
      _this.all(model, filter, options, function (error, data) {
        if (error) {
          if (error.message.toLowerCase().indexOf('connection is closed') > -1) {
            return _this.connect(function () {
              sendResults();
            });
          }
          return observer.onError(error);
        }

        observer.onNext(data);
      });
    };

    if (_.isNumber(feedOptions.throttle)) {
      sendResults = _.throttle(sendResults, feedOptions.throttle);
    }

    sendResults();

    var feed;
    promise = promise.changes(feedOptions.changesOptions);
    if (filter.skip) {
      promise = promise.skip(filter.skip);
    } else if (filter.offset) {
      promise = promise.skip(filter.offset);
    }

    promise.run(client).then(function (res) {
      feed = res;
      var isReady = false;
      feed.eachAsync(function (item) {
        if (item.state === 'ready') {
          isReady = true;
          sendResults();
        } else if (!item.state && isReady) {
          sendResults();
        }
      });
    })
        .catch(function (err) {
          observer.onError(err);
        });

    return function () {
      if (feed) {
        feed.close().catch(function () {});
      }
    };
  });

  return observable;
};

RethinkDB.prototype.all = function all(model, filter, options, callback) {
  var _this = this;
  var client = this.db;

  if (!client) {
    _this.dataSource.once('connected', function () {
      _this.all(model, filter, options, callback);
    });
    return;
  }

  if (!filter) {
    filter = {};
  }

  var promise = r.db(_this.database).table(_this.tableName(model));

  var idName = this.idName(model);

  if (filter.order) {
    var keys = filter.order;
    if (typeof keys === 'string') {
      keys = keys.split(',');
    }
    keys.forEach(function (key) {
      var m = key.match(/\s+(A|DE)SC$/);
      key = key.replace(/\s+(A|DE)SC$/, '').trim();
      if (m && m[1] === 'DE') {
        promise = promise.orderBy(r.desc(key));
      } else {
        promise = promise.orderBy(r.asc(key));
      }
    });
  } else {
    // default sort by id
    if (process.env.TESTING) {
      promise = promise.orderBy(r.asc('_createdAt'));
    } else {
      promise = promise.orderBy({ 'index': r.asc('id') });
    }
  }

  if (filter.where) {
    if (filter.where[idName]) {
      var id = filter.where[idName];
      delete filter.where[idName];
      filter.where.id = id;
    }
    promise = this.buildWhere(model, filter.where, promise);
    if (promise === null)
      return callback && callback(null, []);
  }

  if (filter.skip) {
    promise = promise.skip(filter.skip);
  } else if (filter.offset) {
    promise = promise.skip(filter.offset);
  }

  if (!_.isEmpty(filter.fields)) {
    promise = _this.buildPluck(model, filter.fields, promise);
  }
  
  if (filter.limit) {
    promise = promise.limit(filter.limit);
  }

  var rQuery = promise.toString();

  // console.log(rQuery)

  promise.run(client, function (error, cursor) {
    if (error || !cursor) {
      return callback(error, null);
    }

    var _keys = _this._models[model].properties;
    var _model = _this._models[model].model;

    cursor.toArray(function (err, data) {
      if (err) {
        return callback(err);
      }

      data.forEach(function (element, index) {
        if (element['id'] && idName !== 'id') {
          element[idName] = element['id'];
          delete element['id'];
        }
        _expandResult(element, _keys);
      });

      if (process.env.TESTING) {
        data = data.map(function (item) {
          delete item._createdAt;
          return item;
        });
      }

      if (filter && filter.include) {
        _this._models[model].model.include(data, filter.include, function (err, data) {
          callback(err, data);
        });
      } else {
        callback && callback(null, data, rQuery);
      }
    });
  });
};

RethinkDB.prototype.destroyAll = function destroyAll(model, where, callback) {
  var _this = this;
  var client = this.db;

  if (!client) {
    _this.dataSource.once('connected', function () {
      _this.destroyAll(model, where, callback);
    });
    return;
  }

  if (!callback && 'function' === typeof where) {
    callback = where;
    where = undefined;
  }

  var promise = r.db(_this.database).table(_this.tableName(model));
  if (where !== undefined)
    promise = this.buildWhere(model, where, promise);

  if (promise === null)
    return callback(null, { count: 0 });

  promise.delete().run(client, function (error, result) {
    callback(error, { count: result ? result.deleted : null });
  });
};

RethinkDB.prototype.count = function count(model, where, options, callback) {
  var _this = this;
  var client = this.db;

  callback = callback || function () {};

  if (!client) {
    _this.dataSource.once('connected', function () {
      _this.count(model, where, callback);
    });
    return;
  }

  var promise = r.db(_this.database).table(_this.tableName(model));

  if (where && typeof where === 'object')
    promise = this.buildWhere(model, where, promise);

  if (promise === null)
    return callback(null, 0);

  promise.count().run(client, function (err, count) {
    callback(err, count);
  });
};

RethinkDB.prototype.updateAttributes = function updateAttrs(model, id, data, cb) {
  var _this = this;
  var client = this.db;

  if (!client) {
    _this.dataSource.once('connected', function () {
      _this.updateAttributes(model, id, data, callback);
    });
    return;
  }

  // data.id = id;
  Object.keys(data).forEach(function (key) {
    if (data[key] === undefined)
      data[key] = null;
  });
  r.db(_this.database).table(_this.tableName(model)).get(id).update(data).run(client, function (err, object) {
    cb(err, data);
  });
};

RethinkDB.prototype.update = RethinkDB.prototype.updateAll = function update(model, where, data, callback) {
  var _this = this;
  var client = this.db;

  if (!client) {
    _this.dataSource.once('connected', function () {
      _this.update(model, where, data, callback);
    });
    return;
  }

  var promise = r.db(_this.database).table(_this.tableName(model));
  if (where !== undefined)
    promise = this.buildWhere(model, where, promise);

  if (promise === null)
    return callback && callback(null, { count: 0 });

  Object.keys(data).forEach(function (k) {
    if (data[k].$add) {
      data[k] = r.row(k).add(data[k].$add);
    }
  });

  promise.update(data, { returnChanges: true }).run(client, function (error, result) {
    callback(error, { count: result ? result.replaced : null });
  });
};

RethinkDB.prototype.disconnect = function () {
  this.db.close();
  this.db = null;
};

RethinkDB.prototype.buildWhere = function (model, where, promise) {
  var _this = this;
  if (where === null || (typeof where !== 'object')) {
    return promise;
  }

  var query = this.buildFilter(where, model);

  if (query === undefined)
    return promise;
  else if (query === null)
    return null;
  else
    return promise.filter(query);
};

RethinkDB.prototype.buildFilter = function (where, model) {
  var filter = [];
  var _this = this;

  Object.keys(where).forEach(function (k) {
    // determine if k is field name or condition name
    var conditions = ['and', 'or', 'between', 'gt', 'lt', 'gte', 'lte', 'inq', 'nin', 'near', 'neq', 'like', 'nlike', 'regexp'];
    var condition = where[k];

    if (k === 'and' || k === 'or') {
      if (_.isArray(condition)) {
        var query = _.map(condition, function (c) {
          return _this.buildFilter(c, model);
        });

        if (k === 'and')
          filter.push(_.reduce(query, function (s, f) {
            return s.and(f);
          }));
        else
          filter.push(_.reduce(query, function (s, f) {
            return s.or(f);
          }));
      }
    } else {
      if (_.isObject(condition) && _.intersection(_.keys(condition), conditions).length > 0) {
        // k is condition
        _.keys(condition).forEach(function (operator) {
          if (conditions.indexOf(operator) >= 0) {
            // filter.push(operators[operator](k, condition[operator], _this.getPropertyDefinition.bind(_this, model)))
            filter.push(_this.getCondition(model, k, null, null, condition[operator], operator));
          }
        });
      } else {
        // k is field equality
        filter.push(_this.getCondition(model, k, null, null, condition));
      }
    }
  });

  if (_.findIndex(filter, function (item) { return item === null; }) >= 0)
    return null;

  if (filter.length == 0)
    return undefined;

  return _.reduce(filter, function (s, f) {
    return s.and(f);
  });
};

RethinkDB.prototype.buildPluck = function (model, fields, promise) {
  var pluckObj = fields.reduce(function (acc, fieldName) {
    _.set(acc, fieldName, true);
    return acc;
  }, {});

  return promise.pluck(pluckObj);
};

// Handle nested properties
RethinkDB.prototype.getCondition = function (model, key, relativePath, partialPath, criteria, operator, row) {
  var _this = this;

  row = this.getRow(model, key, relativePath, partialPath, criteria, operator, row);
  if (row.toString().indexOf('contains(') > -1) return row;

  return _this.applyOperator(row, operator, criteria);
};

RethinkDB.prototype.applyOperator = function (row, operator, criteria) {
  var operators = {
    'between': function (row, value) {
      return row.gt(value[0]).and(row.lt(value[1]));
    },
    'gt': function (row, value) {
      if (value === null || value === undefined) return null;
      return row.gt(value);
    },
    'lt': function (row, value) {
      if (value === null || value === undefined) return null;
      return row.lt(value);
    },
    'gte': function (row, value) {
      if (value === null || value === undefined) return null;
      return row.ge(value);
    },
    'lte': function (row, value) {
      if (value === null || value === undefined) return null;
      return row.le(value);
    },
    'inq': function (row, value) {
      var query = [];

      value.forEach(function (v) {
        query.push(row.eq(v));
      });

      var condition = _.reduce(query, function (sum, qq) {
        return sum.or(qq);
      });

      return condition;
    },
    'nin': function (row, value) {
      var query = [];

      value.forEach(function (v) {
        query.push(row.ne(v));
      });

      var condition = _.reduce(query, function (sum, qq) {
        return sum.and(qq);
      });

      return condition;
    },
    'neq': function (row, value) {
      return row.ne(value);
    },
    'like': function (row, value) {
      return row.match(value);
    },
    'nlike': function (row, value) {
      return row.match(value).not();
    },
    'regexp': function (row, value) {
      return row.match(_.trim(value + '', '//'));
    },
  };

  if (operators[operator]) {
    return operators[operator](row, criteria);
  } else {
    return row.eq(criteria);
  }
};

RethinkDB.prototype.getNestedPropertyDefinition = function (model, path) {
  var modelDefinition = this.getModelDefinition(model);
  return _.get(modelDefinition, 'properties.' + path);
};

// Handle nested properties
RethinkDB.prototype.getRow = function (model, key, relativePath, partialPath, criteria, operator, row, isNested) {
  var _this = this;
  var props = (relativePath || key.split('.'));
  partialPath = partialPath || [];

  return props.reduce(function (row, prop, index) {
    partialPath.push(prop);
    var propDef = (_this.getNestedPropertyDefinition(model, partialPath.join('.')) || {});

    if (Array.isArray(propDef) || Array.isArray(propDef.type)) {
      var _relativePath = props.slice(index + 1);
      return row(prop).contains(function (doc) {
        return _this.getCondition(model, key, _relativePath, partialPath, criteria, operator, doc);
      });
    } else {
      if (row.toString().indexOf('contains(') > -1) return row;
      return row(prop);
    }
  }, row || this.r.row);
};

/*
 Some values may require post-processing. Do that here.
 */
function _expandResult(result, keys) {
  Object.keys(result).forEach(function (key) {
    if (!keys.hasOwnProperty(key)) return;

    if (keys[key]['type'] &&
        keys[key]['type']['name'] === 'Date' &&
        !(result[key] instanceof Date)) {
      // Expand date result data, backward compatible
      result[key] = moment.unix(result[key]).toDate();
    }
  });
}

function _hasIndex(_this, model, key) {
  // Primary key always hasIndex
  if (key === 'id') return true;

  var modelDef = _this._models[model];
  return (_.isObject(modelDef.properties[key]) && modelDef.properties[key].index) ||
      (_.isObject(modelDef.settings[key]) && modelDef.settings[key].index);
}

function _toMatchExpr(regexp) {
  var expr = regexp.toString(),
      exprStop = expr.lastIndexOf('/'),
      exprCi = expr.slice(exprStop).search('i');

  expr = expr.slice(1, exprStop);

  if (exprCi > -1) {
    expr = '(?i)' + expr;
  }

  return expr;
}

function _matchFn(k, cond) {
  var matchExpr = _toMatchExpr(cond);
  return function (row) {
    return row(k).match(matchExpr);
  };
}

function _inqFn(k, cond) {
  return function (row) {
    return r.expr(cond).contains(row(k));
  };
}
