var Promise = require('bluebird');
var _       = require('lodash');
var debug   = require('debug')('jsonApi:store:relationaldb');
var Joi     = require('joi');
var util    = require('util');

var SqlStore = module.exports = function SqlStore(config) {
  this.sequelize        = config.sequelize;
  this.baseModel        = config.baseModel;
  this.relations        = config.relations || {};
  this.transactionLevel = config.transactionLevel;

  this.relationArray = [];
  Object.keys(this.relations).forEach(relation => this.relationArray.push(relation));
};

SqlStore._sequelizeInstances = Object.create(null);

/**
  Handlers readiness status. This should be set to `true` once all handlers are ready to process requests.
 */
SqlStore.prototype.ready = false;

SqlStore.prototype.initialise = function (resourceConfig) {
  this.resourceConfig = resourceConfig;
  this.ready = true;
};

SqlStore.prototype.populate = function (callback) {
  var tasks = [
    this.baseModel.sync(),
    Promise.each(this.relationArray, model => model.sync()),
    Promise.each(this.resourceConfig.examples, (exampleJson) => {
      var validation = Joi.validate(exampleJson, this.resourceConfig.attributes);
      if (validation.error) throw validation.error;
      this.create({ request: { type: this.resourceConfig.resource } }, validation.value);
    }),
  ];

  return Promise.each(tasks).asCallback(callback);
};

SqlStore.prototype._fixObject = function (json) {
  var resourceId = `${this.resourceConfig.resource}Id`;

  Object.keys(json).forEach((attribute) => {
    var attributeNamePrefix = `${this.resourceConfig.resource}-`;

    if (attribute.indexOf(attributeNamePrefix) !== 0) return;

    var fixedName = attribute.split(attributeNamePrefix).pop();
    json[fixedName] = json[attribute];

    var val = json[attribute];
    delete json[attribute];
    if (!val) return;

    if (!(val instanceof Array)) val = [val];
    val.forEach((j) => {
      if (j.uid) delete j.uid;
      if (j[resourceId]) delete j[resourceId];
    });
  });

  return json;
};

SqlStore.prototype._errorHandler = function (e, callback) {
  if (e.message && e.message.match(/^ER_LOCK_DEADLOCK/)) {
    return callback({
      status: '500',
      code: 'EMODIFIED',
      title: 'Resource Just Changed',
      detail: 'The resource you tried to mutate was modified by another request. Your request has been aborted.',
    });
  }

  return callback({
    status: '500',
    code: 'EUNKNOWN',
    title: 'An unknown error has occured',
    detail: `Something broke when connecting to the database - ${e.message}`,
  });
};

SqlStore.prototype._generateSearchIncludes = function (relationships) {
  if (!relationships) {
    return {
      count: [],
      findAll: Object.keys(this.relations).map((key) => {
        return this.relations[key];
      }),
    };
  }
  var searchIncludes = Object.keys(this.relations).reduce((partialSearchIncludes, relationName) => {
    var model = this.relations[relationName];
    partialSearchIncludes.findAll.push(model);

    var matchingValue = relationships[relationName];
    if (!matchingValue) return partialSearchIncludes;
    if (matchingValue instanceof Array) {
      matchingValue = matchingValue.filter((i) => {
        return !(i instanceof Object);
      });
      if (!matchingValue.length) return partialSearchIncludes;
    } else if (matchingValue instanceof Object) {
      return partialSearchIncludes;
    }
    var includeClause = {
      model,
      where: { id: matchingValue },
    };
    partialSearchIncludes.count.push(includeClause);
    // replace simple model with clause
    partialSearchIncludes.findAll.pop();
    partialSearchIncludes.findAll.push(includeClause);
    return partialSearchIncludes;
  }, {
    count: [],
    findAll: [],
  });

  return searchIncludes;
};

SqlStore.prototype._generateSearchBlock = function (request) {
  var attributesToFilter = _.omit(request.processedFilter, Object.keys(this.relations));
  var searchBlock = this._getSearchBlock(attributesToFilter);
  return searchBlock;
};

SqlStore.prototype._scalarFilterElementToWhereObj = function (element) {
  var value = element.value;
  var op = element.operator;
  if (!op) return value;

  if (op === '>') return { $gt: value };
  if (op === '<') return { $lt: value };

  var iLikeOperator = '$like';
  if (this.sequelize.getDialect() === 'postgres') iLikeOperator = '$iLike';

  if (op === '~') {
    var caseInsensitiveEqualExpression = { };
    caseInsensitiveEqualExpression[iLikeOperator] = value;
    return caseInsensitiveEqualExpression;
  }

  if (op === ':') {
    var caseInsensitiveContainsExpression = { };
    caseInsensitiveContainsExpression[iLikeOperator] = `%${value}%`;
    return caseInsensitiveContainsExpression;
  }

  return value;
};

SqlStore.prototype._filterElementToSearchBlock = function (filterElement) {
  if (!filterElement) return { };
  var whereObjs = filterElement.map((scalarFilterElement) => {
    return this._scalarFilterElementToWhereObj(scalarFilterElement);
  });
  if (!whereObjs.length) return { };
  if (filterElement.length === 1) {
    return whereObjs[0];
  }
  return { $or: whereObjs };
};

SqlStore.prototype._getSearchBlock = function (filter) {
  if (!filter) return { };
  var searchBlock = { };

  Object.keys(filter).forEach((attributeName) => {
    var filterElement = filter[attributeName];
    searchBlock[attributeName] = this._filterElementToSearchBlock(filterElement);
  });

  return searchBlock;
};

SqlStore.prototype._dealWithTransaction = function (done, callback, existingTransaction) {
  var transactionOptions = {
    isolationLevel: this.transactionLevel,
    autocommit: false,
  };

  var rollback = function (e, transaction) {
    debug('Err', transaction.name, e);
    var a = function () {
      if (e instanceof Error) return this._errorHandler(e, done);
      return done(e);
    };
    transaction.rollback().then(a, a);
  };

  var stopUsingTransaction;
  var t;

  if (existingTransaction) {
    t = { transaction: existingTransaction };

    stopUsingTransaction = function (e) {
      if (e) {
        rollback(e, existingTransaction);
      } else {
        done.apply(null, Array.prototype.slice.call(arguments));
      }
    };

    callback(t, stopUsingTransaction);
  } else {
    this.sequelize.transaction(transactionOptions)
      .then((transaction) => {
        t = { transaction };

        var commit = function () {
          transaction.commit()
            .then(() => done.apply(null, Array.prototype.slice.call(arguments)))
            .catch(err => done(err));
        };

        stopUsingTransaction = function (err) {
          if (err) return rollback(err, transaction);
          return commit.apply(null, Array.prototype.slice.call(arguments));
        };

        return callback(t, stopUsingTransaction);
      });
  }
};

SqlStore.prototype._clearAndSetMany = function (relationModel, prop, theResource, ucKeyName, t) {
  var whereClause = { };
  whereClause[`${theResource.type}Id`] = theResource.id;
  return relationModel
    .destroy({
      where: whereClause,
      transaction: t.transaction,
    })
    .then(() => {
      return Promise.map(prop, (item) => {
        return relationModel.create(item, t)
          .then(newRelationModel => theResource[`add${ucKeyName}`](newRelationModel, t));
      });
    });
};

SqlStore.prototype._clearAndSetOne = function (relationModel, prop, theResource, ucKeyName, t) {
  var whereClause = { };
  whereClause[`${theResource.type}Id`] = theResource.id;
  return relationModel
    .destroy({
      where: whereClause,
      transaction: t.transaction,
    })
    .then(() => {
      if (!prop) {
        return theResource[`set${ucKeyName}`](null, t);
      } else {
        return relationModel.create(prop, t)
          .then((newRelationModel) => {
            return theResource[`set${ucKeyName}`](newRelationModel, t);
          });
      }
    });
};

SqlStore.prototype._clearAndSetRelationTables = function (theResource, partialResource, t, callback) {
  var tasks = { };
  Object.keys(this.relations).forEach((relationName) => {
    var prop = partialResource[relationName];
    if (!Object.prototype.hasOwnProperty.call(partialResource, relationName)) return;
    var relationModel = this.relations[relationName];

    var keyName = `${this.resourceConfig.resource}-${relationName}`;
    var ucKeyName = keyName[0].toUpperCase() + keyName.slice(1, keyName.length);

    tasks[relationName] = function () {
      if (prop instanceof Array) {
        return this._clearAndSetMany(relationModel, prop, theResource, ucKeyName, t);
      } else {
        return this._clearAndSetOne(relationModel, prop, theResource, ucKeyName, t);
      }
    };
  });

  return Promise.props(tasks).asCallback(callback);
};

SqlStore.prototype._generateSearchOrdering = function (request) {
  if (!request.params.sort) return undefined;

  var attribute = request.params.sort;
  var order = 'ASC';
  attribute = String(attribute);
  if (attribute[0] === '-') {
    order = 'DESC';
    attribute = attribute.substring(1, attribute.length);
  }
  return [[attribute, order]];
};

SqlStore.prototype._generateSearchPagination = function (request) {
  var page = request.params.page;
  if (!page) return undefined;

  return {
    limit: page.limit,
    offset: page.offset,
  };
};

/**
  Search for a list of resources, given a resource type.
 */
SqlStore.prototype.search = function (request, callback) {
  var options = { };
  var where = this._generateSearchBlock(request);
  var c;
  if (where) {
    options.where = where;
  }
  var includeBlocks = this._generateSearchIncludes(request.params.filter);
  debug('includeBlocks', util.inspect(includeBlocks, { depth: null }));
  if (includeBlocks.count.length) {
    options.include = includeBlocks.count;
  }
  return this.baseModel.count(options)
    .then((count) => {
      c = count;
      debug('Count', count);
      if (includeBlocks.findAll.length) {
        options.include = includeBlocks.findAll;
      }
      var order = this._generateSearchOrdering(request);
      if (order) {
        options.order = order;
      }
      var pagination = this._generateSearchPagination(request);
      if (pagination) {
        if (pagination.offset > 0 || pagination.limit <= count) {
          _.assign(options, pagination);
        }
      }
      return this.baseModel.findAll(options);
    })
    .then((result) => {
      debug(options, JSON.stringify(result));
      var records = result.map(i => this._fixObject(i.toJSON()));
      debug('Produced', JSON.stringify(records));
      callback(null, records, c);
    })
    .catch((err) => {
      debug(options, err);
      this._errorHandler(err, callback);
    });
};

/**
  Find a specific resource, given a resource type and and id.
 */
SqlStore.prototype.find = function (request, callback) {
  return this.baseModel
    .findOne({
      where: { id: request.params.id },
      include: this.relationArray,
    })
    .then((theResource) => {
      // If the resource doesn't exist, error
      if (!theResource) {
        throw {
          status: '404',
          code: 'ENOTFOUND',
          title: 'Requested resource does not exist',
          detail: `There is no ${request.params.type} with id ${request.params.id}`,
        };
      }

      theResource = this._fixObject(theResource.toJSON());
      debug('Produced', JSON.stringify(theResource));
      callback(null, theResource);
    })
    .catch(err => this._errorHandler(err, callback));
};

/**
  Create (store) a new resource give a resource type and an object.
 */
SqlStore.prototype.create = function (request, newResource, callback) {
  return this._dealWithTransaction(callback, (t, stopUsingTransaction) => {
    return this.baseModel.create(newResource, t)
      .then(theResource => this._clearAndSetRelationTables(theResource, newResource, t))
      .then((theResource) => {
        if (!newResource.id) {
          newResource.id = theResource.id;
        }
      })
      .then(() => stopUsingTransaction(null, newResource))
      .catch(err => stopUsingTransaction(err));
  }, request.dbTransaction);
};

/**
  Delete a resource, given a resource type and and id.
 */
SqlStore.prototype.delete = function (request, callback) {
  return this._dealWithTransaction(callback, (t, stopUsingTransaction) => {
    return this.baseModel
      .findAll({
        where: { id: request.params.id },
        include: this.relationArray,
      })
      .then((results) => {
        var theResource = results[0];

        // If the resource doesn't exist, error
        if (!theResource) {
          throw {
            status: '404',
            code: 'ENOTFOUND',
            title: 'Requested resource does not exist',
            detail: `There is no ${request.params.type} with id ${request.params.id}`,
          };
        }

        return theResource.destroy(t);
      })
      .then(() => stopUsingTransaction())
      .catch(err => stopUsingTransaction(err));
  }, request.dbTransaction);
};

/**
  Update a resource, given a resource type and id, along with a partialResource.
  partialResource contains a subset of changes that need to be merged over the original.
 */
SqlStore.prototype.update = function (request, partialResource, callback) {
  var resource;

  return this._dealWithTransaction(callback, (t, stopUsingTransaction) => {
    return this.baseModel
      .findOne({
        where: { id: request.params.id },
        include: this.relationArray,
        transaction: t.transaction,
      })
      .then((theResource) => {
        resource = theResource;

        // If the resource doesn't exist, error
        if (!theResource) {
          throw {
            status: '404',
            code: 'ENOTFOUND',
            title: 'Requested resource does not exist',
            detail: `There is no ${request.params.type} with id ${request.params.id}`,
          };
        }

        return this._clearAndSetRelationTables(theResource, partialResource, t);
      })
      .then(() => resource.update(partialResource, t))
      .then(() => stopUsingTransaction(null, partialResource))
      .catch(err => stopUsingTransaction(err));
  }, request.dbTransaction);
};
