/* Copyright (c) 2016 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var request = require('request');
var fs = require('fs');
var path = require('path');
var assert = require('assert');
var util = require('util');

function XSClient(req) {
	var oMe = this;
	var trace = makeLog();
	oMe.api = process.env.API_END_POINT;
	oMe.appId = JSON.parse(process.env.VCAP_APPLICATION).application_id;
	trace.log('APP: ' + oMe.appId);

	function _getOptions(callback) {
		assert(callback);
		var oOptions = {
			'json': false, // error messages from XSA are bare strings; not JSON
			'headers': {
				'Content-type': 'application/json'
			}
		};
		if (req.authInfo && req.authInfo.token) {
			oOptions.auth = {
				'bearer': req.authInfo.token
			};
		}
		var ca_certs = process.env.XS_CACERT_PATH;
		if (!ca_certs) {
			callback(null, oOptions);
			return;
		}
		async.map(
			ca_certs.split(path.delimiter),
			function(cert, cert_callback) {
				fs.readFile(cert, 'utf8', cert_callback);
			},
			function(err, ca_list) {
				if (err) {
					callback(err);
					return;
				}
				if (ca_list && ca_list.length > 0) {
					oOptions.agentOptions = {
						'ca': ca_list
					};
				}
				callback(null, oOptions);
			});
	}

	function _request(method, endpoint, body, callback) {
		async.waterfall(
			[
				function(cb1) {
					return oMe.api ? cb1(null) : process.nextTick(cb1, new Error('XSA_NOT_AVAILABLE'));
				},
				_getOptions,
				function(options, cb1) {
					var sep = oMe.api.endsWith('/') ? '' : '/';
					options.uri = oMe.api + sep + 'v2/' + endpoint;
					options.method = method || 'GET';
					options.body = body === undefined ? undefined : JSON.stringify(body);
					trace.log('sending request to XSA API');
					trace.log(JSON.stringify(options));
					request(options, cb1);
				},
				function(res, data, cb1) {
					if (res.statusCode === 401) {
						return cb1(
							null, [{
								msg: 'Unauthorized call to xscontroller: Please use uaa',
								severity: 'error'
							}]
						);
					} else {
						return cb1(
							res.statusCode !== 200 && res.statusCode !== 201 && res.statusCode !== 204 ?
							new Error(
								'Invalid response code: ' + res.statusCode
							) : null,
							data
						);
					}
				}
			],
			callback
		);
	}

	function get(endpoint, callback) {
		_request('GET', endpoint, undefined, callback);
	}
	oMe.getApps = get.bind(oMe, 'apps/' + oMe.appId + '/env');

}

exports.XSClient = XSClient;