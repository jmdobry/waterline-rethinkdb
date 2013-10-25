'use strict';

var _ = require('underscore'),
	gPool = require('generic-pool'),
	async = require('async'),
	r = require('rethinkdb');

module.exports = (function () {
	var defaults = {
			port: 28015,
			host: 'localhost',
			db: 'test',
			authKey: '',
			// I peaked at 2.2k inserts/second with ~500 connections
			poolMax: 500,
			poolMin: 1,
			poolLog: false,
			poolIdleTimeoutMillis: 30000,
			poolRefreshIdle: true,
			poolReapIntervalMillis: 10000,

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
		connection,
		pool = gPool.Pool({
			// right now generic-pool does not support dynamically changing the configuration of a pool
			// so users are stuck with these defaults.
			name: 'rethinkdb',
			max: defaults.poolMax,
			min: defaults.poolMin,
			idleTimeoutMillis: defaults.poolIdleTimeoutMillis,
			log: defaults.poolLog,
			refreshIdle: defaults.poolRefreshIdle,
			reapIntervalMillis: defaults.poolReapIntervalMillis,
			priorityRange: 10,

			create: function (cb) {
				r.connect({
					host: defaults.host,
					port: defaults.port,
					db: defaults.db,
					authKey: defaults.authKey
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

	var RethinkDBAdapter = {

		/**
		 * @method setConfig
		 * @desc Set the default configuration for this adapter.
		 * @param {Object} options The configuration options for this adapter.
		 */
		setConfig: function (options) {
			options = options || {};
			if (!_.isObject(options)) {
				console.error('RethinkDBAdapter.config(options): options: must be an object!');
			} else {
				this.defaults = _.extend(this.defaults, options);
				this.defaults = _.extend(defaults, this.defaults);
			}
		},

		syncable: true,

		// Including a commitLog config enables transactions in this adapter
		// Please note that these are not ACID-compliant transactions:
		// They guarantee *ISOLATION*, and use a configurable persistent store, so they are *DURABLE* in the face of server crashes.
		// However there is no scheduled task that rebuild state from a mid-step commit log at server start, so they're not CONSISTENT yet.
		// and there is still lots of work to do as far as making them ATOMIC (they're not undoable right now)
		//
		// However, for the immediate future, they do a great job of preventing race conditions, and are
		// better than a naive solution.  They add the most value in findOrCreate() and createEach().
		//
		// commitLog: {
		//  identity: '__default_mongo_transaction',
		//  adapter: 'sails-mongo'
		// },

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
			var conn;
			async.waterfall([
				function (next) {
					pool.acquire(next);
				},
				function (connection, next) {
					conn = connection;
					r.tableList().run(conn, next);
				},
				function (tableList, next) {
					var tableExists = false;
					for (var i = 0; i < tableList.length; i++) {
						if (tableList[i] === collectionName) {
							tableExists = true;
							break;
						}
					}
					if (!tableExists) {
						r.tableCreate(collectionName).run(conn, function (err, result) {
							if (err) {
								next(err);
							} else {
								if (result.created !== 1) {
									next('Failed to create table: ' + collectionName);
								} else {
									next();
								}
							}
						});
					} else {
						next();
					}
				}
			], function (err) {
				if (conn) {
					pool.release(conn);
				}
				cb(err);
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
			var conn = connection;
			async.waterfall([
				function (next) {
					pool.acquire(next);
				},
				function (connection, next) {
					conn = connection;
					r.tableList().run(conn, next);
				},
				function (tableList, next) {
					var tableExists = false;
					for (var i = 0; i < tableList.length; i++) {
						if (tableList[i] === collectionName) {
							tableExists = true;
							break;
						}
					}
					if (tableExists) {
						r.tableDrop(collectionName).run(conn, function (err, result) {
							if (err) {
								next(err);
							} else {
								if (result.dropped !== 1) {
									next('Failed to drop table: ' + collectionName);
								} else {
									next();
								}
							}
						});
					} else {
						next();
					}
				}
			], function (err) {
				if (conn) {
					pool.release(conn);
				}
				cb(err);
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
			var conn;
			async.waterfall([
				function (next) {
					pool.acquire(next);
				},
				function (conn, next) {
					conn = connection;
					r.table(collectionName).insert(values, { return_vals: true }).run(conn, function (err, cursor) {
						err ? next(err) : next(null, cursor.new_val);
					});
				}
			], function (err, document) {
				if (conn) {
					pool.release(conn);
				}
				cb(err, document);
			});
		},

		// REQUIRED method if users expect to call Model.find(), Model.findAll() or related methods
		// You're actually supporting find(), findAll(), and other methods here
		// but the core will take care of supporting all the different usages.
		// (e.g. if this is a find(), not a findAll(), it will only send back a single model)
		find: function (collectionName, options, cb) {
			var conn;
			async.waterfall([
				function (next) {
					pool.acquire(next);
				},
				function (connection, next) {
					conn = connection;
					if (options.where) {
						if (options.where.id && options.limit === 1) {
							// findOne
							r.table(collectionName).get(options.where.id).run(conn, function (err, document) {
								err ? cb(err) : next(null, [document]);
							});
						} else {
							// findAll w/query
							r.table(collectionName).filter(options.where).run(conn, function (err, cursor) {
								err ? cb(err) : cursor.toArray(next);
							});
						}
					} else {
						// findAll
						r.table(collectionName).run(conn, function (err, cursor) {
							err ? cb(err) : cursor.toArray(next);
						});
					}
				}
			], function (err, documents) {
				if (conn) {
					pool.release(conn);
				}
				cb(err, documents);
			});
		},

		// REQUIRED method if users expect to call Model.update()
		update: function (collectionName, options, values, cb) {
			var conn;
			async.waterfall([
				function (next) {
					pool.acquire(next);
				},
				function (connection, next) {
					conn = connection;
					if (options.where) {
						if (options.where.id) {
							r.table(collectionName).get(options.where.id).update(values, { return_vals: true }).run(conn, function (err, cursor) {
								err ? cb(err) : next(null, [cursor.new_val]);
							});
						} else {
							cb('not supported');
						}
					} else {
						cb('not supported');
					}
				}
			], function (err, documents) {
				if (conn) {
					pool.release(conn);
				}
				cb(err, documents);
			});
		},

		// REQUIRED method if users expect to call Model.destroy()
		destroy: function (collectionName, options, cb) {
			var conn;
			async.waterfall([
				function (next) {
					pool.acquire(next);
				},
				function (connection, next) {
					conn = connection;
					if (options.where) {
						if (options.where.id) {
							r.table(collectionName).get(options.where.id).delete().run(conn, function (err) {
								err ? cb(err) : next();
							});
						} else {
							r.table(collectionName).filter(options.where).delete().run(conn, function (err) {
								err ? cb(err) : next();
							});
						}
					} else {
						r.table(collectionName).delete().run(conn, function (err) {
							err ? cb(err) : next();
						});
					}
				}
			], function (err) {
				if (conn) {
					pool.release(conn);
				}
				cb(err);
			});
		}

		// REQUIRED method if users expect to call Model.stream()
//		stream: function (collectionName, options, stream) {
//			// options is a standard criteria/options object (like in find)
//
//			// stream.write() and stream.end() should be called.
//			// for an example, check out:
//			// https://github.com/balderdashy/sails-dirty/blob/master/DirtyAdapter.js#L247
//		}

		/*
		 **********************************************
		 * Optional overrides
		 **********************************************

		 // Optional override of built-in batch create logic for increased efficiency
		 // otherwise, uses create()
		 createEach: function (collectionName, cb) { cb(); },

		 // Optional override of built-in findOrCreate logic for increased efficiency
		 // otherwise, uses find() and create()
		 findOrCreate: function (collectionName, cb) { cb(); },

		 // Optional override of built-in batch findOrCreate logic for increased efficiency
		 // otherwise, uses findOrCreate()
		 findOrCreateEach: function (collectionName, cb) { cb(); }
		 */


		/*
		 **********************************************
		 * Custom methods
		 **********************************************

		 ////////////////////////////////////////////////////////////////////////////////////////////////////
		 //
		 // > NOTE:  There are a few gotchas here you should be aware of.
		 //
		 //    + The collectionName argument is always prepended as the first argument.
		 //      This is so you can know which model is requesting the adapter.
		 //
		 //    + All adapter functions are asynchronous, even the completely custom ones,
		 //      and they must always include a callback as the final argument.
		 //      The first argument of callbacks is always an error object.
		 //      For some core methods, Sails.js will add support for .done()/promise usage.
		 //
		 //    +
		 //
		 ////////////////////////////////////////////////////////////////////////////////////////////////////
		 */
	};

	return RethinkDBAdapter;
})();