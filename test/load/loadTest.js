'use strict';
var RethinkDBAdapter = require('../../lib/adapter'),
	assert = require('assert'),
	async = require('async'),
	Waterline = require('waterline');

var INSERTS = 5000,
	BATCH_INSERTS = 1000,
	testCollection;

var adapter = new RethinkDBAdapter({
	max: 300,
	reapIntervalMillis: 10000
});

describe('Load Testing', function () {
	// test should finish in under 1 minute
	this.timeout(120000);

	before(function (done) {
		adapter.drop('test', function (err) {
			if (err) {
				console.error(err);
				return done(err);
			} else {

				var Test = Waterline.Collection.extend({
					adapter: 'rethinkdb',
					tableName: 'test',

					autoPK: false,

					attributes: {
						last_name: 'integer',
						first_name: 'integer',
						email: 'integer'
					}
				});

				new Test({
					adapters: {
						rethinkdb: adapter
					}
				}, function (err, collection) {
					if (err) {
						throw new Error(err);
					} else {
						testCollection = collection;
					}
					done(err);
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
				adapter.create('test', data, next);
			}, function (err, users) {
				assert(!err);
				assert(users.length === INSERTS);
				done();
			});
		});

		it('should not error', function (done) {
			// generate x users
			async.times(BATCH_INSERTS, function (n, next) {
				testCollection.createEach('test', [
					{'last_name': 4291, 'first_name': 55653, 'email': 88348},
					{'last_name': 20092, 'first_name': 57194, 'email': 38757},
					{'last_name': 32883, 'first_name': 91660, 'email': 75342},
					{'last_name': 25467, 'first_name': 99436, 'email': 3973},
					{'last_name': 46075, 'first_name': 78705, 'email': 21921},
					{'last_name': 40805, 'first_name': 39111, 'email': 67858},
					{'last_name': 64012, 'first_name': 85343, 'email': 28328},
					{'last_name': 49431, 'first_name': 49130, 'email': 27043},
					{'last_name': 52280, 'first_name': 91705, 'email': 77296},
					{'last_name': 12134, 'first_name': 22382, 'email': 85148},
					{'last_name': 89119, 'first_name': 71893, 'email': 18171},
					{'last_name': 34702, 'first_name': 11393, 'email': 81793},
					{'last_name': 34409, 'first_name': 43763, 'email': 37404},
					{'last_name': 69934, 'first_name': 55081, 'email': 59309},
					{'last_name': 98240, 'first_name': 14900, 'email': 31970},
					{'last_name': 20367, 'first_name': 7434, 'email': 62213},
					{'last_name': 90281, 'first_name': 81068, 'email': 60135},
					{'last_name': 20068, 'first_name': 27613, 'email': 40157},
					{'last_name': 31736, 'first_name': 25784, 'email': 3363},
					{'last_name': 54660, 'first_name': 97835, 'email': 24318},
					{'last_name': 98348, 'first_name': 88107, 'email': 55358},
					{'last_name': 50359, 'first_name': 13093, 'email': 86307},
					{'last_name': 80496, 'first_name': 10171, 'email': 23663},
					{'last_name': 40938, 'first_name': 59374, 'email': 22602},
					{'last_name': 35187, 'first_name': 38236, 'email': 32556},
					{'last_name': 909, 'first_name': 56448, 'email': 43026},
					{'last_name': 6029, 'first_name': 47929, 'email': 48786},
					{'last_name': 10540, 'first_name': 93273, 'email': 25999},
					{'last_name': 48421, 'first_name': 97539, 'email': 45132},
					{'last_name': 65387, 'first_name': 58531, 'email': 26533},
					{'last_name': 23528, 'first_name': 27804, 'email': 28665},
					{'last_name': 48168, 'first_name': 26080, 'email': 3705},
					{'last_name': 27354, 'first_name': 77273, 'email': 25565},
					{'last_name': 36552, 'first_name': 83236, 'email': 16192},
					{'last_name': 97034, 'first_name': 53061, 'email': 66735},
					{'last_name': 48220, 'first_name': 33449, 'email': 81647},
					{'last_name': 12450, 'first_name': 82545, 'email': 73578},
					{'last_name': 25089, 'first_name': 69506, 'email': 25032},
					{'last_name': 87595, 'first_name': 67733, 'email': 25241},
					{'last_name': 11156, 'first_name': 86848, 'email': 30515}
				], next);
			}, function (err) {
				assert(!err);
				done();
			});
		});
	});
});
