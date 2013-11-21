'use strict';
/**
 * Wipe Database before tests are run and
 * after tests are run to ensure a clean test environment.
 */
var RethinkDBAdapter = require('../../../lib/adapter'),
	config = require('./config');

var adapter = new RethinkDBAdapter();

function dropTable(cb) {
	adapter.registerCollection({ identity: 'test', config: config }, function (err) {
		if (err) {
			cb(err);
		}
		adapter.drop('test', cb);
	});
}

// Global Before Helper
before(function (done) {
	dropTable(done);
});

// Global After Helper
after(function (done) {
	dropTable(done);
});