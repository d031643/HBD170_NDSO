'use strict';
var async = require('async');
var helpers = require('./api/helpers');

function checkResponse(response, body, cb) {
	if (!response || !response.statusCode) {
		return cb(new Error('Invalid response'));
	} else if (response.statusCode < 200 || response.statusCode >= 300) {
		return cb(new Error('Invalid response: ' + response.statusMessage, response));
	} else {
		return cb(null);
	}
}
exports.doIt = function(cb) {
	async.waterfall(
		[
			cb1 => helpers.getTOEClient().registerTaskGroup('nDso', 'Process Native DataStores', cb1),
			checkResponse,
			cb1 => helpers.getTOEClient().registerTaskGroup('flowGraph', 'Process FlowGraphs', cb1),
			checkResponse
		],
		cb
	);
};