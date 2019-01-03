/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
//var request = require( 'request' );

exports.doIt = function(oTrace, cb) {

	oTrace.info('Get datastore feature');

	var result = {
		isDataStoreActive: false
	};

	if (!process.env.dwf_runtime_ui_configs && !process.env.datastore_feature_configs) {
		return process.nextTick(cb, null, result);
	}

	let config = null;
	//check dwf runtime is installed - first
	try {
		config = JSON.parse(process.env.dwf_runtime_ui_configs);
	} catch (e) {
		return process.nextTick(cb, e);
	}

	if (!config || config.length === 0) {
		//check datastore feature is installed -- second - backward compatibility
		try {
			config = JSON.parse(process.env.datastore_feature_configs);
		} catch (e) {
			return process.nextTick(cb, e);
		}
		if (!config) {
			return process.nextTick(cb, null, result);
		}
	}

	async.detect(config, (item, task_callback) => {
		oTrace.info('Trying URL: ' + item.url);
		/* we should verify that the url is correct, but we cannot right now because
		 * no application user authorization token is available
		request.get( item.url , ( ping_err, ping_response ) => {
		    oTrace.info( 'Result of URL: ' + ping_response );
		    task_callback( !ping_err && ping_response.statusCode === 200 );
		});
		 */

		return task_callback(!!item.url);
	}, (found) => {
		oTrace.info('Datastore found: ' + JSON.stringify(found));
		result.isDataStoreActive = found !== undefined;
		return cb(null, result);
	});
};