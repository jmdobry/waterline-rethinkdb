'use strict';

var _ = require('underscore'),
	gPool = require('generic-pool'),
	async = require('async'),
	r = require('rethinkdb');

module.exports = (function () {
	//noinspection JSValidateTypes
	var defaults = {
			port: 28015,
			host: 'localhost',
			db: 'test',
			authKey: '',
			// I peaked at 2.3k inserts/second in loadTest.js with ~500 connections
			poolMax: 20,
			poolMin: 1,
			poolLog: false,
			poolIdleTimeoutMillis: 30000,
			poolRefreshIdle: true,
			poolReapIntervalMillis: 1000,

			// If setting syncable, you should consider the migrate option,
			// which allows you to set how the sync will be performed.
			// It can be overridden globally in an app (config/adapters.js) and on a per-model basis.
			//
			// drop   => Drop schema and data, then recreate it
			// alter  => Drop/add columns as necessary, but try
			// safe   => Don't change anything (good for production DBs)
			migrate: 'alter'
		},
		definitions = {},
		pool = createPool(defaults);

	if (process.env.CI) {
		setInterval(function () {
			console.log('waitingClientsCount: ' + pool.waitingClientsCount());
		}, 1000);
	}

	var RethinkDBAdapter = {

		/**
		 * @method setConfig
		 * @desc Set the default configuration for this adapter.
		 * @param {Object} options The configuration options for this adapter.
		 * @param {Boolean} strict If true, reset configuration to defaults before applying new configuration.
		 */
		setConfig: function (options, strict) {
			options = options || {};
			if (!_.isObject(options)) {
				console.error('RethinkDBAdapter.config(options): options: must be an object!');
			} else {
				if (strict) {
					this.defaults = _.clone(defaults);
				}
				this.defaults = _.extend(this.defaults, options);
				var oldPool = pool;
				oldPool.drain();
				pool = createPool(this.defaults);
			}
		},

		syncable: true,

		// Default configuration for collections
		// override these by calling adapter.setConfig(options)
		defaults: _.clone(defaults),

		// This method runs when a model is initially registered at server start time
		registerCollection: function (collection, cb) {
			var self = this;
			if (!(collection.identity in definitions)) {
				definitions[collection.identity] = _.clone(collection);
			}
			delete definitions[collection.identity].id;
			self.define(collection.identity, definitions[collection.identity], function (err) {
				if (err) {
					cb(err);
				} else {
					cb(null, definitions[collection.identity]);
				}
			});
		},

		// The following methods are optional
		////////////////////////////////////////////////////////////

		// Optional hook fired when a model is unregistered, typically at server halt
		// useful for tearing down remaining open connections, etc.
		teardown: function (cb) {
			pool.drain(function () {
				pool.destroyAllNow();
				cb();
			});
		},

		// REQUIRED method if integrating with a schemaful database
		define: function (collectionName, definition, cb) {
			delete definitions[collectionName].id;
			var self = this;
			// Remove any autoIncement keys, RethinkDB won't support them without
			// a hacky additional collection
			for (var key in definition) {
				if (definition.hasOwnProperty(key)) {
					for (var k in definition[key]) {
						if (definition.hasOwnProperty(key)) {
							if (definition[key][k].autoIncrement) {
								delete definition[key][k].autoIncrement;
							}
						}
					}
				}
			}
			this.run(r.tableList(), function (err, tableList) {
				var tableExists = false;
				if (tableList) {
					for (var i = 0; i < tableList.length; i++) {
						if (tableList[i] === collectionName) {
							tableExists = true;
							break;
						}
					}
				}
				if (!tableExists) {
					var options = {};

					// Detect if the primary key should be something other than 'id'
					for (var k in definitions[collectionName].definition) {
						var attribute = definitions[collectionName].definition[k];
						if (attribute.primaryKey) {
							options.primary_key = k;
							break;
						}
					}

					self.run(r.tableCreate(collectionName, options), function (err, result) {
						if (err) {
							cb(err);
						} else if (result.created !== 1) {
							cb('Failed to create table: ' + collectionName);
						} else {
							var queries = [];
							// Create a secondary index for each attribute that needs a uniqueness constraint
							for (var j in definitions[collectionName].definition) {
								var attribute = definitions[collectionName].definition[j];
								if (attribute.unique && !attribute.primaryKey) {
									queries.push((function (index) {
										return function (cb) {
											self.run(r.table(collectionName).indexCreate(index), cb);
										};
									})(j));
								}
							}
							if (queries.length) {
								async.series(queries, cb);
							} else {
								cb();
							}
						}
					});
				} else {
					cb();
				}
			});
		},

		// REQUIRED method if integrating with a schemaful database
		describe: function (collectionName, cb) {
			var schema = Object.keys(definitions[collectionName].definition).length === 0 ?
				null : definitions[collectionName].definition;
			return cb(null, schema);
		},

		// REQUIRED method if integrating with a schemaful database
		drop: function (collectionName, cb) {
			var self = this;
			this.run(r.tableList(), function (err, tableList) {
				var tableExists = false;
				if (tableList) {
					for (var i = 0; i < tableList.length; i++) {
						if (tableList[i] === collectionName) {
							tableExists = true;
							break;
						}
					}
				}
				if (tableExists) {
					self.run(r.tableDrop(collectionName), function (err, result) {
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
		},

		// Optional override of built-in alter logic
		// Can be simulated with describe(), define(), and drop(),
		// but will probably be made much more efficient by an override here
		// alter: function (collectionName, attributes, cb) {
		// Modify the schema of a table or collection in the data store
		// cb();
		// },

		// REQUIRED method if users expect to call Model.create() or any methods
		create: function (collectionName, values, cb) {
			var queries = [], _this = this;

			// Check uniqueness constraint for all unique attributes
			// The table must have a secondary index for each unique attribute
			for (var k in definitions[collectionName].definition) {
				var attribute = definitions[collectionName].definition[k];
				if (attribute.unique && !attribute.primaryKey) {
					queries.push((function (index) {
						return function (cb) {
							_this.run(r.table(collectionName).getAll(values[index] || '', { index: index }).count(), function (err, result) {
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
					})(k));
				}
			}

			if (queries.length) {
				async.series(queries, insert);
			} else {
				insert();
			}

			function insert(err) {
				if (err) {
					cb(err);
				} else {
					_this.run(r.table(collectionName).insert(values, { return_vals: true }), function (err, cursor) {
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
		},

		// REQUIRED method if users expect to call Model.find(), Model.findAll() or related methods
		// You're actually supporting find(), findAll(), and other methods here
		// but the core will take care of supporting all the different usages.
		// (e.g. if this is a find(), not a findAll(), it will only send back a single model)
		find: function (collectionName, options, cb) {
			if (options.where) {
				if (options.where.id && options.limit === 1) {
					// findOne
					this.run(r.table(collectionName).get(options.where.id), function (err, document) {
						err ? cb(err) : cb(null, [document]);
					});
				} else {
					// findAll w/query
					this.run(r.table(collectionName).filter(options.where), function (err, cursor) {
						err ? cb(err) : cursor.toArray(cb);
					});
				}
			} else {
				// findAll
				this.run(r.table(collectionName), function (err, cursor) {
					err ? cb(err) : cursor.toArray(cb);
				});
			}
		},

		count: function (collectionName, options, cb) {
			if (options.where) {
				if (options.where.id && options.limit === 1) {
					this.run(r.table(collectionName).get(options.where.id).count(), cb);
				} else {
					this.run(r.table(collectionName).filter(options.where).count(), cb);
				}
			} else {
				this.run(r.table(collectionName).count(), cb);
			}
		},

		// REQUIRED method if users expect to call Model.update()
		update: function (collectionName, options, values, cb) {
			if (options.where) {
				if (options.where.id) {
					this.run(r.table(collectionName).get(options.where.id).update(values, { return_vals: true }), function (err, cursor) {
						err ? cb(err) : cb(null, [cursor.new_val]);
					});
				} else {
					cb('Batch update not implemented yet.');
				}
			} else {
				cb('Batch update not implemented yet.');
			}
		},

		// REQUIRED method if users expect to call Model.destroy()
		destroy: function (collectionName, options, cb) {
			if (options.where) {
				if (options.where.id) {
					this.run(r.table(collectionName).get(options.where.id).delete(), cb);
				} else {
					this.run(r.table(collectionName).filter(options.where).delete(), cb);
				}
			} else {
				this.run(r.table(collectionName).delete(), cb);
			}
		},

		// REQUIRED method if users expect to call Model.stream()
//		stream: function (collectionName, options, stream) {
//			// options is a standard criteria/options object (like in find)
//
//			// stream.write() and stream.end() should be called.
//			// for an example, check out:
//			// https://github.com/balderdashy/sails-dirty/blob/master/DirtyAdapter.js#L247
//		}

		// **********************************************
		// * Optional overrides
		// **********************************************

		// Optional override of built-in batch create logic for increased efficiency
		// otherwise, uses create()
		createEach: function (collectionName, values, cb) {
			var self = this;
			this.run(r.table(collectionName).insert(values), function (err, result) {
				if (err) {
					cb(err);
				} else {
					// Previous query:
					//   var query = r.table(collectionName);
					//   query.getAll.apply(query, result.generated_keys)
					// Which is better?
					self.run(r.expr(result.generated_keys).map(r.table(collectionName).get(r.row)), function (err, cursor) {
						if (err) {
							cb(err);
						} else {
							cursor.toArray(function (err, documents) {
								if (err) {
									cb(err);
								} else {
									cb(null, documents)
								}
							});
						}
					});
				}
			});
		},

		// Optional override of built-in findOrCreate logic for increased efficiency
		// otherwise, uses find() and create()
//		 findOrCreate: function (collectionName, cb) { cb(); },

		// Optional override of built-in batch findOrCreate logic for increased efficiency
		// otherwise, uses findOrCreate()
//		 findOrCreateEach: function (collectionName, cb) { cb(); }

		run: function (query, cb) {
			var conn;
			async.waterfall([
				function (next) {
					pool.acquire(next);
				},
				function (connection, next) {
					conn = connection;
					query.run(conn, next);
				}
			], function (err, result) {
				if (conn) {
					pool.release(conn);
				}
				cb(err, result);
			});
		}
	};

	/**
	 * @method createPool
	 * @desc Create a new connection pool based on the current default options.
	 * @param {Object} options The configuration options.
	 * @returns {Object}
	 */
	function createPool(options) {
		options = options || {};
		options = _.extend(defaults, options);
		//noinspection JSValidateTypes
		return gPool.Pool({
			name: 'rethinkdb',
			max: options.poolMax,
			min: options.poolMin,
			idleTimeoutMillis: options.poolIdleTimeoutMillis,
			log: options.poolLog,
			refreshIdle: options.poolRefreshIdle,
			reapIntervalMillis: options.poolReapIntervalMillis,
			priorityRange: 10,

			create: function (cb) {
				r.connect({
					host: options.host,
					port: options.port,
					db: options.db,
					authKey: options.authKey
				}, function (err, conn) {
					if (err) {
						cb(err);
					} else {
						cb(null, conn);
					}
				});
			},

			destroy: function (conn) {
				conn.close();
			}
		});
	}

	return RethinkDBAdapter;
})();