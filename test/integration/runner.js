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