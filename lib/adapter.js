'use strict';

var _ = require('underscore'),
	r = require('rethinkdb');

module.exports = (function () {
	var definitions = {},
		connection;

	var RethinkDBAdapter = {

		/**
		 * @method config
		 * @desc Set the default configuration for this adapter.
		 * @param {Object} options The configuration options for this adapter.
		 */
		config: function (options) {
			options = options || {};
			if (!_.isObject(options)) {
				console.error('RethinkDBAdapter.config(options): options: must be an object!');
			} else {
				this.defaults = _.extend(this.defaults, options);
			}
			reopenConnection(this.defaults, function () {
				console.log('RethinkDBAdapter.config(options): Reconnected to database.');
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
		// override these by calling adapter.config(options)
		defaults: {
			port: 28015,
			host: 'localhost',
			database: '',
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
			if (!(collection.identity in definitions)) {
				definitions[collection.identity] = _.clone(collection);
			}
			console.log('registerCollection(collection, cb)', collection);
			cb(collection);
		},

		// The following methods are optional
		////////////////////////////////////////////////////////////

		// Optional hook fired when a model is unregistered, typically at server halt
		// useful for tearing down remaining open connections, etc.
		teardown: function (cb) {
			console.log('tearDown(cb)');
			cb();
		},

		// REQUIRED method if integrating with a schemaful database
		define: function (collectionName, definition, cb) {
			console.log('define(collectionName, definition, cb)', collectionName, definition);
			// Define a new "table" or "collection" schema in the data store
			cb();
		},

		// REQUIRED method if integrating with a schemaful database
		describe: function (collectionName, cb) {
			console.log('describe(collectionName, cb)', collectionName);
			// Respond with the schema (attributes) for a collection or table in the data store
			var attributes = {};
			cb(null, attributes);
		},

		// REQUIRED method if integrating with a schemaful database
		drop: function (collectionName, cb) {
			console.log('drop(collectionName, cb)', collectionName);
			// Drop a "table" or "collection" schema from the data store
			cb();
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
			console.log('create(collectionName, cb)', collectionName, values);
			// Create a single new model specified by values

			// Respond with error or newly created model instance
			cb(null, values);
		},

		// REQUIRED method if users expect to call Model.find(), Model.findAll() or related methods
		// You're actually supporting find(), findAll(), and other methods here
		// but the core will take care of supporting all the different usages.
		// (e.g. if this is a find(), not a findAll(), it will only send back a single model)
		find: function (collectionName, options, cb) {
			console.log('find(collectionName, cb)', collectionName, options);

			// ** Filter by criteria in options to generate result set

			// Respond with an error or a *list* of models in result set
			cb(null, []);
		},

		// REQUIRED method if users expect to call Model.update()
		update: function (collectionName, options, values, cb) {
			console.log('update(collectionName, cb)', collectionName, options, values);

			// ** Filter by criteria in options to generate result set

			// Then update all model(s) in the result set

			// Respond with error or a *list* of models that were updated
			cb();
		},

		// REQUIRED method if users expect to call Model.destroy()
		destroy: function (collectionName, options, cb) {
			console.log('destroy(collectionName, cb)', collectionName, options);

			// ** Filter by criteria in options to generate result set

			// Destroy all model(s) in the result set

			// Return an error or nothing at all
			cb();
		},

		// REQUIRED method if users expect to call Model.stream()
		stream: function (collectionName, options, stream) {
			console.log('stream(collectionName, cb)', collectionName, options, stream);
			// options is a standard criteria/options object (like in find)

			// stream.write() and stream.end() should be called.
			// for an example, check out:
			// https://github.com/balderdashy/sails-dirty/blob/master/DirtyAdapter.js#L247

		}

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