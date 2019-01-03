/* Copyright (c) 2016 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';
var _ = require('lodash');
var async = require('async');
var request = require('request');
var xsenv = require('@sap/xsenv');
var ExitHandler = require('./ExitHandler');
var trace = require('./trace');
var Messages = require('./Messages').Messages;

function TOEClient() {
	var oMe = this;

	function handleFinalResponse(e) {
		if (e) {
			trace.log('Error in transferring state');
			trace.log(e.message);
			trace.log(e.stack);
		} else {
			trace.log('state transferred to TOE');
		}
		return e;
	}
	var o = xsenv.cfServiceCredentials({
		tag: 'dwf'
	});
	if (!o) {
		throw new Error('Invalid TOE instance');
	}
	if (!o.providers_v1) {
		throw new Error('Invalid TOE instance');
	}
	if (!o.providers_v1.length) {
		throw new Error('Invalid TOE instance');
	}
	var toeProvider = o.providers_v1.find(p => p.name === 'dwf-toe');
	if (!toeProvider) {
		throw new Error('Invalid TOE instance');
	}
	if (!toeProvider.url) {
		throw new Error('Invalid TOE instance');
	}
	if (!o.user || !typeof(o.user) === 'string') {
		throw new Error('Invalid TOE instance');
	}
	if (!o.password || !typeof(o.password) === 'string') {
		throw new Error('Invalid TOE instance');
	}
	var getUrl = _.constant(toeProvider.url);
	var getUser = _.constant(o.user);
	var getPassword = _.constant(o.password);

	function checkResponse(err, response, body) {
		if (err) {
			trace.log('The response contains ans error');
			trace.logObj(err);
			throw err;
		}
		if (response.statusCode < 200 || response.statusCode > 299) {
			trace.log('Unexpected TOE response: ' + response.statusCode);
			trace.log(response.statusMessage);
			trace.logObj(body);
			throw (new Error('Unexpected TOE response: ' + response.statusMessage));
		}
		return body;
	}

	oMe.sendTOEMsg = function(req, severity, msg, cb) {
		trace.log('In send toe msg');
		trace.log('Sending to TOE');
		request({
				uri: req.body.callbackUri,
				json: true,
				method: 'PUT',
				auth: {
					user: getUser(),
					password: getPassword(),
					sendImmediately: true
				},
				rejectUnauthorized: false,
				body: {
					status: 'ACTIVE',
					subStatus: '',
					messages: [{
						severity: severity,
						msg: msg,
						timestamp: (new Date()).toString()
					}]
				}
			},
			(err, response, body) => oMe.checkResponse(err, response, body, (err1) => cb(err1))
		);
	};
	oMe.sendAnswer = (req, cb) => async.waterfall(
		[
			cb1 => {
				trace.log('Sending cancellation to: ' + req.body.callbackUri);
				return cb1();
			},
			cb1 => request({
					uri: req.body.callbackUri, //swagger def guaranties this to exist
					json: true,
					method: 'PUT',
					auth: {
						user: getUser(),
						password: getPassword(),
						sendImmediately: true
					},
					rejectUnauthorized: false,
					body: {
						status: 'ERROR',
						subStatus: '',
						logUri: null,
						messages: [{
							severity: 'error',
							msg: 'Service shutdown',
							timestamp: (new Date()).toString()
						}]
					}
				},
				cb1
			)
		],
		(err, response, body) => {
			if (err) {
				trace.log('TOE answered with error: ');
				trace.log(err.statusCode + '\t' + err.statusMessge);
				trace.log(body);
			}
			return cb(err, err ? 'Failed to notify toe' : 'Aborted task ');
		}
	);
	oMe.sendRequest = function(sUrl, sMethod, cb) {
		request({
				uri: getUrl() + sUrl,
				json: true,
				method: sMethod,
				auth: {
					user: getUser(),
					password: getPassword(),
					sendImmediately: true
				},
				rejectUnauthorized: false
			},
			(err, response, body) => oMe.checkResponse(err, response, body, (err1) => cb(err1, body))
		);
	};
	oMe.sendRequestWithBody = function(sUrl, sMethod, oBody, cb) {
		var s = getUrl() + sUrl;
		request({
				uri: s,
				json: true,
				method: sMethod,
				auth: {
					user: getUser(),
					password: getPassword(),
					sendImmediately: true
				},
				body: oBody,
				rejectUnauthorized: false
			},
			(err, response, body) => oMe.checkResponse(err, response, body, (err1) => cb(err1, body))
		);
	};

	function createReqForTOE(err, req) {
		var aMsg = [];
		if (err) {
			if (err instanceof(Messages)) {
				aMsg = err.getMessages();
			} else {
				aMsg = (new Messages()).addError(err).getMessages();
			}
		}
		return {
			uri: req.body.callbackUri,
			json: true,
			method: 'PUT',
			auth: {
				user: getUser(),
				password: getPassword(),
				sendImmediately: true
			},
			rejectUnauthorized: false,
			body: {
				status: err ? 'ERROR' : 'OK',
				subStatus: '',
				logUri: null,
				messages: aMsg
			}
		};
	}
	oMe.handleTOEResponse = function(err, req, n) {
		trace.log('handle TOEResponse...');
		if (err) {
			trace.log('Error occured');
			trace.logObj(err);
		}
		var o1 = createReqForTOE(err, req);
		trace.log('Sending response to:' + o1.uri);
		request(
			o1,
			(err1, response, body) => oMe.checkResponse(err1, response, body, handleFinalResponse)
		);
		ExitHandler.removeHandler(n);
	};

	function attachExitHandler(res, req, s) {
		res.status(202)
			.json({
				status: 'ACTIVE',
				subStatus: '',
				logUri: null,
				messages: [{
					msg: 'Starting to process',
					severity: 'info'
				}]
			});
		return ExitHandler.addHandler(function(cb) {
			trace.log('Send abort to TOE of task: ' + s);
			exports.getTOEClient().sendAnswer(req, cb);
		});
	}
	oMe.executeTask = function(req, res, s, a, cb) {
		trace.log('executing:' + s);
		var n = attachExitHandler(
			res,
			req,
			s
		);
		trace.log('Executing ' + a.length + ' subtasks');
		async.waterfall(
			a.map(function(f, i) {
				return function() {
					trace.log('Executing subfunction ' + i);
					return f.apply(this, arguments);
				};
			}),
			err => {
				trace.log('Exexution of ' + s + ' finished ');
				trace.log(err ? ' with error.' : ' successfull.');
				exports.getTOEClient().handleTOEResponse(err, req, n);
				if (cb) {
					cb.apply(this, arguments);
				}
			}
		);
	};
	oMe.checkResponse = function(e, r, b, c) {
		var o1 = null;
		var e3 = null;
		try {
			o1 = checkResponse(e, r, b);
		} catch (e2) {
			e3 = e2;
		}
		return c(e3, o1);
	};
	oMe.registerTaskGroup = function(sGroup, sDescription, cb) {
		var sUrl = getUrl() + '/taskGroup/' + sGroup;
		trace.log('registering group: ' + sGroup + ' as ' + sUrl);
		trace.log(sUrl);
		request({
			uri: sUrl,
			json: true,
			method: 'POST',
			auth: {
				user: getUser(),
				password: getPassword(),
				sendImmediately: true
			},
			rejectUnauthorized: false,
			body: {
				serviceUri: JSON.parse(
					process.env.VCAP_APPLICATION
				).full_application_uris[0] + '/backend/taskType/' + sGroup,
				description: sDescription,
				iconUri: 'sap-icon://folder'
			}
		}, cb);
	};
}
exports.getTOEClient = _.constant(new TOEClient());