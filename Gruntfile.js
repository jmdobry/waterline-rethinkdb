'use strict';
module.exports = function (grunt) {

	require('load-grunt-tasks')(grunt);
	require('time-grunt')(grunt);

	var config = {
		lib: 'lib',
		test: 'test'
	};

	grunt.initConfig({
		config: config,
		jshint: {
			options: {
				jshintrc: '.jshintrc'
			},
			all: [
				'Gruntfile.js',
				'<%= config.lib %>/{,*/}*.js',
				'<%= config.test %>/{,*/}*.js'
			]
		}
	});

	grunt.registerTask('build', [
		'jshint'
	]);

	grunt.registerTask('default', [
		'build'
	]);

	grunt.registerTask('test', ['build']);

	grunt.registerTask('travis', ['build']);
};