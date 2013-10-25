'use strict';
var RethinkDBAdapter = require('../../lib/adapter'),
	Config = require('../support/config'),
	Fixture = require('../support/fixture'),
	assert = require('assert'),
	async = require('async');

var INSERTS = 30000;

describe('Load Testing', function () {
	// test should finish in under 1 minute
	this.timeout(60000);

	before(function (done) {
		var Schema;

		RethinkDBAdapter.drop('test', function (err) {
			if (err) {
				console.error(err);
				return done(err);
			} else {

				// Register The Collection
				RethinkDBAdapter.registerCollection({ identity: 'test', config: Config }, function (err) {
					if (err) {
						console.error(err);
						done(err);
					} else {
						// Define The Collection
						RethinkDBAdapter.define('test', Fixture, function (err, schema) {
							if (err) {
								console.error(err);
								done(err);
							} else {
								Schema = schema;
								done();
							}
						});
					}
				});
			}
		});
	});

	describe('create with x connection', function () {
		it('should not error', function (done) {
			// generate x users
			async.times(INSERTS, function (n, next) {
				var data = {
					first_name: Math.floor((Math.random() * 100000) + 1),
					last_name: Math.floor((Math.random() * 100000) + 1),
					email: Math.floor((Math.random() * 100000) + 1)
				};
				RethinkDBAdapter.create('test', data, next);
			}, function (err, users) {
				assert(!err);
				assert(users.length === INSERTS);
				done();
			});
		});
	});
});
