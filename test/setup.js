'use strict';

var r = require('rethinkdb'),
	async = require('async'),
	connection;

function setup(err, tries) {
	if (tries > 1) {
		throw new Error(err);
	} else {
		async.waterfall([
			function (next) {
				console.log('Connecting to test database');
				setTimeout(function () {
					r.connect({
						host: '127.0.0.1',
						port: 28015,
						db: 'test'
					}, next);
				}, 20);
			},
			function (conn, next) {
				connection = conn;
				console.log('Connected to test database');
				console.log('Creating test tables');
				r.tableCreate('post').run(connection, next);
			},
			function (result, next) {
				if (result.created === 0) {
					next('Failed to create "post" table');
				} else {
					console.log(result);
					r.tableCreate('comment').run(connection, next);
				}
			},
			function (result, next) {
				if (result.created === 0) {
					next('Failed to create "comment" table');
				} else {
					console.log(result);
					r.tableCreate('user').run(connection, next);
				}
			},
			function (result, next) {
				if (result.created === 0) {
					next('Failed to create "user" table');
				} else {
					console.log(result);
					connection.close();
				}
			}
		], function (err) {
			if (err) {
				setup(err, 2);
			}
		});
	}
}

setup(null, 1);
