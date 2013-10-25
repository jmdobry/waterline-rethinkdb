'use strict';
/**
 * Run Integration Tests
 *
 * Uses the waterline-adapter-tests module to
 * run mocha tests against the currently implemented
 * waterline API.
 */
var tests = require('../../node_modules/waterline-adapter-tests'),
	RethinkDBAdapter = require('../../lib/adapter');

/**
 * Build a RethinkDB Config File
 */
RethinkDBAdapter.setConfig({
	db: 'test',
	host: '127.0.0.1',
	port: 28015
}, function (err, config) {
	if (err) {
		throw new Error(err);
	} else {
		console.log('Config set to:');
		console.log(config);
	}
});

var config = {
	db: 'test',
	host: '127.0.0.1',
	port: 28015
};

/**
 * Run Tests
 */
new tests({
	adapter: RethinkDBAdapter,
	config: config,
	failOnError: true
});