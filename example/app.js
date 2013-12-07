'use strict';

var express = require('express'),
	app = require('express')(),
	RethinkDBAdapter = require('waterline-rethinkdb'),
	r = require('rethinkdb'),
	Waterline = require('waterline');

// This configuration hash also accepts all of the same options accepted by https://github.com/coopernurse/node-pool
// The defaults for the connection pool can be found in node_modules/waterline-rethinkdb/lib/connection
var options = {
	db: 'test',
	host: '127.0.0.1',
	port: 28015,
	authKey: 'myPassword'
};

var rethinkDBAdapter = new RethinkDBAdapter(options);

// RethinkDBAdapter#connectionRun executes any arbitrary ReQL query.
// connectionRun(query, cb) uses a connection from the adapter's connection pool.
rethinkDBAdapter.connectionRun(r.tableList(), function (err, result) {
	console.log('result'); // [ 'test' ]
});

var UserModel = Waterline.Collection.extend({
	adapter: 'rethinkdb',
	tableName: 'user',

	autoPK: false,

	attributes: {
		email: {
			type: 'email',
			required: true
		},
		username: {
			type: 'string',
			required: true
		},
		firstName: 'string',
		lastName: 'string'
	}
});

var User;

new UserModel({
	adapters: {
		rethinkdb: rethinkDBAdapter
	}
}, function (err, collection) {
	if (err) {
		throw new Error(err);
	} else {
		User = collection;
		rethinkDBAdapter.connectionRun(r.tableList(), function (err, result) {
			console.log('result'); // [ 'test', 'user' ]
		});
	}
	cb(err, collection);
});

app.use(express.json());
app.use(express.urlencoded());
app.use(express.methodOverride());
app.use(app.router);

require('http').createServer(app).listen(3000);
