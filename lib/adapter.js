'use strict';

var _ = require('underscore'),
	async = require('async'),
	r = require('rethinkdb');

module.exports = (function () {
	var definitions = {},
		connection;

	var RethinkDBAdapter = {

		/**
		 * @method setConfig
		 * @desc Set the default configuration for this adapter.
		 * @param {Object} options The configuration options for this adapter.
		 * @param {Function} cb Callback function.
		 */
		setConfig: function (options, cb) {
			var self = this;
			options = options || {};
			if (!_.isObject(options)) {
				console.error('RethinkDBAdapter.config(options): options: must be an object!');
			} else {
				this.defaults = _.extend(this.defaults, options);
			}
			reopenConnection(this.defaults, function (err) {
				if (err) {
					cb(err);
				} else {
					cb(null, self.defaults);
				}
			});
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
		defaults: {
			port: 28015,
			host: 'localhost',
			db: 'test',
			authKey: '',

			// If setting syncable, you should consider the migrate option,
			// which allows you to set how the sync will be performed.
			// It can be overridden globally in an app (config/adapters.js) and on a per-model basis.
			//
			// drop   => Drop schema and data, then recreate it
			// alter  => Drop/add columns as necessary, but try
			// safe   => Don't change anything (good for production DBs)
			migrate: 'alter'
		},

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
			cb();
		},

		// REQUIRED method if integrating with a schemaful database
		define: function (collectionName, definition, cb) {
			delete definitions[collectionName].id;
			async.waterfall([
				function (next) {
					getConnection({}, next);
				},
				function (conn, next) {
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
						getConnection({}, function (err, conn) {
							if (err) {
								next(err);
							} else {
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
							}
						});
					} else {
						next();
					}
				}
			], cb);
		},

		// REQUIRED method if integrating with a schemaful database
		describe: function (collectionName, cb) {
			var schema = Object.keys(definitions[collectionName].definition).length === 0 ?
				null : definitions[collectionName].definition;
			return cb(null, schema);
		},

		// REQUIRED method if integrating with a schemaful database
		drop: function (collectionName, cb) {
			async.waterfall([
				function (next) {
					getConnection({}, next);
				},
				function (conn, next) {
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
						getConnection({}, function (err, conn) {
							if (err) {
								next(err);
							} else {
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
							}
						});
					} else {
						next();
					}
				}
			], cb);
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
			async.waterfall([
				function (next) {
					getConnection({}, next);
				},
				function (conn, next) {
					r.table(collectionName).insert(values, { return_vals: true }).run(conn, function (err, cursor) {
						err ? next(err) : next(null, cursor.new_val);
					});
				}
			], cb);
		},

		// REQUIRED method if users expect to call Model.find(), Model.findAll() or related methods
		// You're actually supporting find(), findAll(), and other methods here
		// but the core will take care of supporting all the different usages.
		// (e.g. if this is a find(), not a findAll(), it will only send back a single model)
		find: function (collectionName, options, cb) {
			async.waterfall([
				function (next) {
					getConnection({}, next);
				},
				function (conn, next) {
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
			], cb);
		},

		// REQUIRED method if users expect to call Model.update()
		update: function (collectionName, options, values, cb) {
			async.waterfall([
				function (next) {
					getConnection({}, next);
				},
				function (conn, next) {
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
			], cb);
		},

		// REQUIRED method if users expect to call Model.destroy()
		destroy: function (collectionName, options, cb) {
			async.waterfall([
				function (next) {
					getConnection({}, next);
				},
				function (conn, next) {
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
			], cb);
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


		 // Any other methods you include will be available on your models
		 foo: function (collectionName, cb) {
		 cb(null,"ok");
		 },
		 bar: function (collectionName, baz, watson, cb) {
		 cb("Failure!");
		 }


		 // Example success usage:

		 Model.foo(function (err, result) {
		 if (err) console.error(err);
		 else console.log(result);

		 // outputs: ok
		 })

		 // Example error usage:

		 Model.bar(235, {test: 'yes'}, function (err, result){
		 if (err) console.error(err);
		 else console.log(result);

		 // outputs: Failure!
		 })

		 */
	};

	/**
	 * @method closeConnection
	 * @desc Close the current connection if it exists.
	 */
//	function closeConnection() {
//		if (connection) {
//			connection.close();
//		}
//	}

	/**
	 * @method reopenConnection
	 * @desc Reopen the current connection or create it if it doesn't exist. Right now we're maintaining a single connection.
	 * @param {Object} options Configuration options for connecting to the database.
	 * @param {Function} cb Callback function.
	 */
	function reopenConnection(options, cb) {
		options = options || {};
		if (!cb || !_.isFunction(cb)) {
			console.error('You must provide a callback!');
			return;
		} else if (!_.isObject(options)) {
			cb('reopenConnection(options, cb): options: must be an object!');
		}
		options = _.extend(options, RethinkDBAdapter.defaults);

		if (connection) {
			connection.reconnect(function (err, conn) {
				if (err) {
					cb(err);
				} else {
					connection = conn;
					cb(null, connection);
				}
			});
		} else {
			getConnection(options, function (err, conn) {
				if (err) {
					cb(err);
				} else {
					connection = conn;
					cb(null, connection);
				}
			});
		}
	}

	/**
	 * @method getConnection
	 * @desc Return the active connection or create it if it doesn't exist. Right now we're maintaining a single connection.
	 * @param {Object} options Configuration options for connecting to the database.
	 * @param {Function} cb Callback function.
	 */
	function getConnection(options, cb) {
		options = options || {};
		if (!cb || !_.isFunction(cb)) {
			console.error('You must provide a callback!');
			return;
		} else if (!_.isObject(options)) {
			cb('getConnection(options, cb): options: must be an object!');
		}
		options = _.extend(options, RethinkDBAdapter.defaults);

		if (!connection) {
			r.connect({
				host: options.host,
				port: options.port,
				db: options.db,
				authKey: options.authKey
			}, function (err, conn) {
				if (err) {
					cb(err);
				} else {
					connection = conn;
					cb(null, connection);
				}
			});
		} else {
			cb(null, connection);
		}
	}

	return RethinkDBAdapter;
})();

//////////////                 //////////////////////////////////////////
////////////// Private Methods //////////////////////////////////////////
//////////////                 //////////////////////////////////////////