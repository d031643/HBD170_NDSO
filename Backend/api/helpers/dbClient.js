'use strict';
var async = require('async');
var xsenv = require('@sap/xsenv');
var hdbext = require('@sap/hdbext');
var trace = require('./trace');

exports.createDBClientPair = function(cb) {
	if (typeof(cb) !== 'function') {
		throw new Error('Invalid callback');
	}
	var oClient1 = null;
	var svs = xsenv.cfServiceCredentials({
		tag: 'hana'
	});
	if (!svs && !svs.credentials && !svs.credentials.schema) {
		return cb(new Error('failed to create database client pair '));
	}
	return async.waterfall(
		[
			cb1 => hdbext.createConnection(svs, cb1),
			(oClient, cb1) => {
				oClient1 = oClient;
				return cb1(null);
			},
			cb1 => hdbext.createConnection(svs, cb1)
		],
		(oErr, oClient) => cb(
			oErr, {
				client1: oClient1,
				client2: oClient,
				schema: svs.schema
			}
		)
	);
};
exports.closeDBClientPair = function(o) {
	try {
		if (o) {
			if (o.client1) {
				o.client1.end();
			}
			if (o.client2) {
				o.client2.end();
			}
		}
	} catch (error) {
		trace.error('Error in client close: ' + error.message);
	}
};