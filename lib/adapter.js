'use strict';

var _ = require('underscore'),
	async = require('async'),
	Connection = require('./connection'),
	r = require('rethinkdb');

/**
 * Default configuration.
 * @type {{
 * migrate: string
 * }}
 */
var defaults = {
	// If setting syncable, you should consider the migrate option,
	// which allows you to set how the sync will be performed.
	// It can be overridden globally in an app (config/adapters.js) and on a per-model basis.
	//
	// drop   => Drop schema and data, then recreate it
	// alter  => Drop/add columns as necessary, but try
	// safe   => Don't change anything (good for production DBs)
	migrate: 'alter'
};

/**
 * Constructs a new RethinkDBAdapter instance.
 *
 * A RethinkDBAdapter instance provides and interface for interacting with a Rethink database.
 *
 * @param {object} options Configuration options for the new instance.
 * @constructor
 */
function RethinkDBAdapter(options) {

	///////////////////////
	// Private variables //
	///////////////////////
	var collections = {},
		connection = new Connection(options);

	///////////////////////
	// Private functions //
	///////////////////////
	function _getDef(collectionName) {
		return collections[collectionName];
	}

	function _setDef(collectionName, definition) {
		collections[collectionName] = definition;
	}

	/**
	 * Configure this RethinkDBAdapter instance with the given options.
	 * @param {object} options Configuration options for this instance.
	 * @param {boolean} [strict] If true, reset configuration to the defaults before applying the new options.
	 * @private
	 */
	function _configure(options, strict) {
		var errorPrefix = 'RethinkDBAdapter.configure(options, strict): options';

		if (!_.isObject(options)) {
			throw new Error(errorPrefix + ': must be an object!');
		} else if (options.migrate && !_.isString(options.migrate)) {
			throw new Error(errorPrefix + '.migrate: must be a string!');
		}

		connection.configure(options, strict);
	}

	function _connectionRun(query, cb) {
		connection.run(query, cb);
	}

	function _connectionDrain(cb) {
		connection.drain(cb);
	}

	function _connectionDestroyAllNow() {
		connection.destroyAllNow();
	}

	////////////////////
	// Public methods //
	////////////////////
	this.getDef = _getDef;
	this.setDef = _setDef;
	this.configure = _configure;
	this.definitions = function () {
		return collections;
	};

	// Methods which allow us to keep our connection completely private.
	this.connectionRun = _connectionRun;
	this.connectionDrain = _connectionDrain;
	this.connectionDestroyAllNow = _connectionDestroyAllNow;

	///////////
	// Setup //
	///////////
	this.syncable = true;
	this.defaults = _.clone(defaults);
}

/**
 * Register a Collection with this adapter. This will create the necessary table if it doesn't exist.
 * @param {object} newCollection New Collection to register with this adapter.
 * @param {function} cb Callback function.
 */
RethinkDBAdapter.prototype.registerCollection = function registerCollection(newCollection, cb) {

	var collectionName = newCollection.identity,
		collection = this.getDef(collectionName);

	if (!collection) {
		collection = _.clone(newCollection);
		this.setDef(collectionName, collection);
	}
	delete collection.id;

	this.define(collectionName, collection, function (err) {
		if (err) {
			cb(err);
		} else {
			cb(null, collection);
		}
	});
};

/**
 * Close all connections. Called when a Collection is unregistered.
 * @param {function} cb Callback function.
 */
RethinkDBAdapter.prototype.teardown = function teardown(cb) {

	var _this = this;

	_this.connectionDrain(function () {
		_this.connectionDestroyAllNow();
		cb();
	});
};

/**
 * Create the table in the database for the given collection.
 * @param {string} collectionName Name of collection whose table is to be created.
 * @param {object} definition Collection definition.
 * @param {function} cb Callback function.
 */
RethinkDBAdapter.prototype.define = function define(collectionName, definition, cb) {

	var collection = this.getDef(collectionName),
		_this = this,
		tableCreateOptions = {},
		secondaryIndexQueries = [];

	delete definition.id;

	// Remove any autoIncement keys, RethinkDB won't support them without
	// a hacky additional collection
	for (var key in definition) {
		if (definition.hasOwnProperty(key)) {
			for (var k in definition[key]) {
				if (definition[key].hasOwnProperty(k)) {
					if (definition[key][k].autoIncrement) {
						delete definition[key][k].autoIncrement;
					}
				}
			}
		}
	}

	collection.secondary_indices = collection.secondary_indices || [];

	for (var schemaKey in collection.definition) {
		if (collection.definition.hasOwnProperty(schemaKey)) {
			var attribute = collection.definition[schemaKey];
			// Detect if the primary key should be something other than 'id'
			if (attribute.primaryKey && !collection.primaryKey) {
				collection.primaryKey = schemaKey;
			}
			// Detect the secondary indices (for the uniqueness constraint) that need to be created
			if (attribute.unique && !attribute.primaryKey) {
				collection.secondary_indices.push(schemaKey);
				secondaryIndexQueries.push((function (index) {
					return function (cb) {
						_this.connectionRun(r.table(collectionName).indexCreate(index), cb);
					};
				})(schemaKey));
			}
		}
	}
	tableCreateOptions.primaryKey = collection.primaryKey || 'id';

	// Create the table if it doesn't exist
	async.waterfall([
		function (next) {
			_this.connectionRun(r.tableList(), next);
		},
		function (tableList, next) {
			if (tableList && _.indexOf(tableList, collectionName) === -1) {
				_this.connectionRun(r.tableCreate(collectionName, tableCreateOptions), function (err, result) {
					if (err) {
						next(err);
					} else if (result.created !== 1) {
						next('Failed to create table: ' + collectionName);
					} else {
						next();
					}
				});
			} else {
				next();
			}
		}
	], function (err) {
		if (err) {
			cb(err);
		} else if (secondaryIndexQueries.length) {
			async.series(secondaryIndexQueries, cb);
		} else {
			cb();
		}
	});

};

/**
 * Return the schema of the given collection.
 * @param {string} collectionName Name of collection whose schema is to be returned.
 * @param {function} cb Callback function.
 * @returns {*} Schema of the given collection.
 */
RethinkDBAdapter.prototype.describe = function describe(collectionName, cb) {

	var collection = this.getDef(collectionName);
	var schema = Object.keys(collection.definition).length === 0 ?
		null : collection.definition;
	return cb(null, schema);
};

/**
 * Drop the table for the given collection from the database.
 * @param {string} collectionName Collection whose table should be dropped.
 * @param {function} cb Callback function.
 */
RethinkDBAdapter.prototype.drop = function drop(collectionName, cb) {

	var _this = this;

	// Drop the table if it exists
	this.connectionRun(r.tableList(), function (err, tableList) {
		if (tableList && _.indexOf(tableList, collectionName) !== -1) {
			_this.connectionRun(r.tableDrop(collectionName), function (err, result) {
				if (err) {
					cb(err);
				} else if (result.dropped !== 1) {
					cb('Failed to drop table: ' + collectionName);
				} else {
					cb();
				}
			});
		} else {
			cb();
		}
	});
};

/**
 * Create a new record in the given collection.
 * @param {string} collectionName Name of collection of new record.
 * @param {object} values Attributes for new record.
 * @param {function} cb Callback function.
 */
RethinkDBAdapter.prototype.create = function create(collectionName, values, cb) {

	var queries = [],
		collection = this.getDef(collectionName),
		_this = this;

	function insert(err) {
		if (err) {
			cb(err);
		} else {
			_this.connectionRun(r.table(collectionName).insert(values, { return_vals: true }), function (err, cursor) {
				if (err) {
					cb(err);
				} else if (cursor.errors !== 0) {
					cb(cursor.first_error || 'insert failed');
				} else {
					cb(null, cursor.new_val);
				}
			});
		}
	}

	// Check uniqueness constraint for all unique attributes
	// The table must have a secondary index for each unique attribute
	for (var key in collection.definition) {
		if (collection.definition.hasOwnProperty(key)) {
			var attribute = collection.definition[key];
			if (attribute.unique && !attribute.primaryKey) {
				queries.push((function (index) {
					return function (cb) {
						_this.connectionRun(r.table(collectionName).getAll(values[index] || '', { index: index }).count(), function (err, result) {
							if (err) {
								cb(err);
							} else {
								if (result > 0) {
									cb('Unique constraint failure!');
								} else {
									cb();
								}
							}
						});
					};
				})(key));
			}
		}
	}

	if (queries.length) {
		async.series(queries, insert);
	} else {
		insert();
	}
};

RethinkDBAdapter.prototype.find = function find(collectionName, options, cb) {

	var collection = this.getDef(collectionName),
		_this = this;

	options = options || {};
	options.where = options.where || {};

	var single = false;
	// Detect whether we can use a single-row operation
	if (options.where[collection.primaryKey || 'id']) {
		single = 'primaryKey';
	} else {
		_.each(collection.secondary_indices, function (secondary_index) {
			if (options.where[secondary_index]) {
				single = secondary_index;
			}
		});
	}

	if (single) {
		if (single === 'primaryKey') {
			// Find the record based on the collection's primary key
			this.connectionRun(r.table(collectionName).get(options.where[collection.primaryKey || 'id']), function (err, document) {
				if (err) {
					cb(err);
				} else {
					cb(null, [document]);
				}
			});
		} else {
			// Find the record based on one of the secondary indices we found.
			this.connectionRun(r.table(collectionName).get(options.where[single], {
				index: single
			}), function (err, document) {
				if (err) {
					cb(err);
				} else {
					cb(null, [document]);
				}
			});
		}
	} else {
		// Could not use a single-row operation, have to use a filter
		async.waterfall([
			function (next) {
				_this.connectionRun(r.table(collectionName).filter(options.where), next);
			},
			function (cursor, next) {
				cursor.toArray(next);
			}
		], cb);
	}
};

/**
 * Return the number of records in the given collection that meet the given criteria.
 * @param {string} collectionName Name of the collection to search.
 * @param {object} options Options for search. Can include a criteria object.
 * @param {function} cb Callback function.
 */
RethinkDBAdapter.prototype.count = function count(collectionName, options, cb) {

	var collection = this.getDef(collectionName);

	options = options || {};
	options.where = options.where || {};

	if (options.where) {
		if (options.where[collection.primaryKey || 'id'] && options.limit === 1) {
			this.connectionRun(r.table(collectionName).get(options.where[collection.primaryKey || 'id']).count(), cb);
		} else {
			this.connectionRun(r.table(collectionName).filter(options.where).count(), cb);
		}
	} else {
		this.connectionRun(r.table(collectionName).count(), cb);
	}
};

RethinkDBAdapter.prototype.update = function update(collectionName, options, values, cb) {

	var collection = this.getDef(collectionName),
		_this = this;

	if (_.isArray(values)) {
		var queries = [];
		_.each(values, function (value) {
			queries.push((function (val) {
				return function (local_cb) {
					collection.update(collectionName, options, val, local_cb);
				};
			})(value));
		});
		// Perform parallel queries and merge together the results
		async.parallel(queries, cb);
	} else {
		options = options || {};
		options.where = options.where || {};
		var single = false;

		// We need to detect whether we can use the RethinkDB .get(...) method immediately,
		// or if we first need to use .filter(...)
		if (options.where[collection.primaryKey || 'id']) {
			single = 'primaryKey';
		} else {
			_.each(collection.secondary_indices, function (secondary_index) {
				if (options.where[secondary_index]) {
					single = secondary_index;
				}
			});
		}

		if (single) {
			if (single === 'primaryKey') {
				// Update the record by its primary key
				async.waterfall([
					function (next) {
						_this.connectionRun(r.table(collectionName).get(options.where[collection.primaryKey || 'id']).update(values, { returnVals: true }), function (err, cursor) {
							if (err) {
								next(err);
							} else if (cursor.errors > 0) {
								next(cursor.first_error);
							} else {
								next(null, cursor.new_val);
							}
						});
					}
				], cb);
			} else {
				// Update the record by a secondary index
				async.waterfall([
					function (next) {
						_this.connectionRun(r.table(collectionName).get(options.where[single], {
							index: single
						}).update(values, { returnVals: true }), function (err, cursor) {
							if (err) {
								next(err);
							} else if (cursor.errors > 0) {
								next(cursor.first_error);
							} else {
								next(null, cursor.new_val);
							}
						});
					}
				], cb);
			}
		} else {
			// Here the user gave us a criteria object that does not resolve to a unique item.
			// So we retrieve the primary keys of the rows in the table that meet the criteria,
			// then call adapter.update(...) on each individual key–resulting in recursion 1-level deep.
			async.waterfall([
				function (next) {
					_this.connectionRun(r.table(collectionName).filter(options.where).pluck(collection.primaryKey || 'id'), next);
				},
				function (cursor, next) {
					cursor.toArray(next);
				},
				function (ids, next) {
					var queries = [];
					_.each(ids, function (id) {
						queries.push((function (primaryKey) {
							return function (local_cb) {
								collection.update(collectionName, {
									where: primaryKey
								}, values, local_cb);
							};
						})(id));
					});
					// Perform parallel queries and merge together the results
					async.parallel(queries, next);
				}
			], cb);
		}
	}
};

/**
 * Delete a record or many records from the database.
 * @param {string} collectionName Name of collection from which to delete the record(s);
 * @param {object} options Options for the query. May include a criteria object.
 * @param {function} cb Callback function.
 */
RethinkDBAdapter.prototype.destroy = function destroy(collectionName, options, cb) {

	var collection = this.getDef(collectionName);

	options = options || {};
	options.where = options.where || {};

	if (options.where) {
		if (options.where[collection.primaryKey || 'id']) {
			this.connectionRun(r.table(collectionName).get(options.where[collection.primaryKey || 'id']).delete(), cb);
		} else {
			this.connectionRun(r.table(collectionName).filter(options.where).delete(), cb);
		}
	} else {
		this.connectionRun(r.table(collectionName).delete(), cb);
	}
};

RethinkDBAdapter.prototype.createEach = function createEach(collectionName, values, cb) {

	var _this = this;

	async.waterfall([
		function (next) {
			_this.connectionRun(r.table(collectionName).insert(values), next);
		},
		function (result, next) {
			_this.connectionRun(r.expr(result.generated_keys).map(r.table(collectionName).get(r.row)), next);
		},
		function (cursor, next) {
			cursor.toArray(next);
		}
	], cb);
};

// TODO: RethinkDBAdapter.prototype.findOrCreate
// TODO: RethinkDBAdapter.prototype.findOrCreateEach

module.exports = RethinkDBAdapter;
