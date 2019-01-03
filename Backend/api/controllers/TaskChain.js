'use strict';
var async = require('async');
var fs = require('fs');
var request = require('request');
var dsoTasks = require('./dataStore/datastoreTasks');
var fgTasks = require('./flowGraph/flowGraphTasks');
var helpers = require('../helpers');
var trace = helpers.trace;

function extractDescr(s) {
	var a = s.match('^.*\\.(.*)$');
	return a ? a[1] : s;
}

function handleResponse(res, err, o) {
	if (err) {
		res.status(500).json({
			msg: err.message,
			stack: err.stack
		});
	} else {
		res.send(o);
	}
}

module.exports = {
	getDataStores4VH: function(req, res) {
		var oCP = null;
		async.waterfall(
			[
				helpers.dbClient.createDBClientPair,
				function(oClientPair, cb1) {
					oCP = oClientPair;
					return dsoTasks.getDataStores(
						trace,
						oCP.client1,
						oCP.schema,
						cb1
					);
				},
				(a, cb1) => cb1(
					undefined,
					a.result.map(
						ds => {
							return {
								value: ds,
								displayText: extractDescr(ds)
							};
						}
					)
				)
			],
			(err, o) => {
				helpers.dbClient.closeDBClientPair(oCP);
				handleResponse(res, err, o);
			}
		);
	},
	getFiles: function(req, res) {
		async.waterfall(
			[
				cb1 => fs.readdir(process.cwd(), cb1),
				(a, cb1) => async.map(
					a,
					(fn, cb2) => fs.stat(
						fn,
						function(err, file) {
							if (err) {
								return cb2(err);
							} else {
								return cb2(
									null, {
										name: fn,
										isDir: file.isDirectory()
									}
								);
							}
						}
					),
					(e, a1) => {
						if (e) {
							return cb1(e);
						} else {
							return cb1(
								null,
								a1.filter(
									o => !o.isDirectory
								).map(o => {
									return {
										value: process.cwd() + '/' + o.name,
										displayText: process.cwd() + '/' + o.name
									};
								})
							);
						}
					}
				)
			],
			function(err, a) {
				handleResponse(res, err, a);
			}
		);

	},
	getTaskTypesNdso: function(req, res) {
		trace.log('get types for ndso');
		res.send(
			[{
				id: 'activate',
				iconUri: 'sap-icon://favorite',
				description: 'Activate nDso',
				subStatusIsDynamic: false,
				maxPrecondition: 1,
				inlineEnabled: true,
				referenceEnabled: false,
				parameterModel: {
					dataStoreName: {
						type: 'string',
						description: 'Data Store Name',
						inputHelp: '/dataStoreName/inputHelp',
						obligatory: true
					}
				},
				status: {
					'OK': {
						'description': 'Status ok'
					},
					'ERROR': {
						'description': 'Status error'
					}
				}
			}, {
				id: 'loadSQL',
				iconUri: 'sap-icon://favorite',
				description: 'Load via SQL',
				subStatusIsDynamic: false,
				maxPrecondition: 1,
				inlineEnabled: true,
				referenceEnabled: false,
				parameterModel: {
					dataStoreName: {
						type: 'string',
						description: 'Data Store Name',
						inputHelp: '/dataStoreName/inputHelp',
						controlType: 'COMBOBOX',
						obligatory: true
					},
					inboundTable: {
						type: 'string',
						description: 'Name of the inbound table',
						inputHelp: '/inboundTable/inputHelp',
						controlType: 'COMBOBOX'
					},
					sql: {
						type: 'string',
						description: 'query with a resultset that fits into inbound queue'
					}
				},
				status: {
					'OK': {
						'description': 'Status ok'
					},
					'ERROR': {
						'description': 'Status error'
					}
				}
			}, {
				id: 'loadFile',
				iconUri: 'sap-icon://favorite',
				description: 'Load a file',
				subStatusIsDynamic: false,
				maxPrecondition: 1,
				inlineEnabled: true,
				referenceEnabled: false,
				parameterModel: {
					dataStoreName: {
						type: 'string',
						description: 'Data Store Name',
						inputHelp: '/dataStoreName/inputHelp',
						controlType: 'COMBOBOX',
						obligatory: true
					},
					inboundTable: {
						type: 'string',
						description: 'Name of the inbound table',
						inputHelp: '/inboundTable/inputHelp',
						controlType: 'COMBOBOX'
					},
					fileName: {
						type: 'string',
						description: 'Name of the file to be loaded',
						inputHelp: '/fileName/inputHelp',
						controlType: 'COMBOBOX'
					},
					withHeaderLine: {
						type: 'boolean',
						description: 'first line contains the field names'
					}
				},
				status: {
					'OK': {
						'description': 'Status ok'
					},
					'ERROR': {
						'description': 'Status error'
					}
				}
			}, {
				id: 'loadHTTP',
				iconUri: 'sap-icon://favorite',
				description: 'Load from a URL',
				subStatusIsDynamic: false,
				maxPrecondition: 1,
				inlineEnabled: true,
				referenceEnabled: false,
				parameterModel: {
					dataStoreName: {
						type: 'string',
						description: 'Data Store Name',
						inputHelp: '/dataStoreName/inputHelp',
						controlType: 'COMBOBOX',
						obligatory: true
					},
					inboundTable: {
						type: 'string',
						description: 'Name of the inbound table',
						inputHelp: '/inboundTable/inputHelp',
						controlType: 'COMBOBOX'
					},
					withHeaderLine: {
						type: 'boolean',
						description: 'first line contains the fieldnames'
					},
					url: {
						type: 'string',
						description: 'Url to be loaded'
					},
					proxy: {
						type: 'string',
						description: 'Proxy to be used'
					},
					timeout: {
						type: 'int',
						description: 'timeout in milliseconds'
					}
				},
				status: {
					'OK': {
						'description': 'Status ok'
					},
					'ERROR': {
						'description': 'Status error'
					}
				}
			}]
		);
	},
	getTaskTypesFlowGraph: function(req, res) {
		trace.log('get types for flowgraph');
		res.send(
			[{
				id: 'execute',
				iconUri: 'sap-icon://favorite',
				description: 'Execute FlowGraph',
				subStatusIsDynamic: false,
				maxPrecondition: 1,
				inlineEnabled: true,
				referenceEnabled: false,
				parameterModel: {
					flowGraphName: {
						type: 'string',
						description: 'FlowGraph Name',
						inputHelp: '/flowGraphName/inputHelp',
						obligatory: true
					}
				},
				status: {
					'OK': {
						'description': 'Status ok'
					},
					'ERROR': {
						'description': 'Status error'
					}
				}
			}]
		);
	},
	getFlowGraphVH: function(req, res) {
		var oCP = null;
		async.waterfall(
			[
				helpers.dbClient.createDBClientPair,
				(oClientPair, cb1) => {
					oCP = oClientPair;
					return fgTasks.getFlowGraphs(
						trace,
						oCP.client1,
						oCP.schema,
						cb1
					);
				},
				(a, cb1) => cb1(null, a.map(
					fg => {
						return {
							displayText: extractDescr(fg),
							value: fg
						};
					}
				))
			],
			(err, a) => {
				helpers.dbClient.closeDBClientPair(oCP);
				handleResponse(res, err, a);
			}
		);
	},
	activateRequests: function(req, res) {
		var oCP = null;
		helpers.getTOEClient().executeTask(
			req, res, 'activateRequests', [
				cb1 => helpers.getTOEClient().sendTOEMsg(
					req,
					'info',
					'activate requests of datastore: ' + req.body.parameterValues.dataStoreName,
					cb1
				),
				helpers.dbClient.createDBClientPair,
				(oClientPair, cb1) => {
					oCP = oClientPair;
					return dsoTasks.getRequestsForActivation(
						trace,
						oCP.client1,
						oCP.schema,
						req.body.parameterValues.dataStoreName,
						200000000,
						cb1
					);
				},
				(o, cb1) => cb1(
					o && o.result && o.result.length ? null : new Error('failed to retrieve request list'),
					o.result.map(oR => oR.requestId)
				),
				(a, cb1) => helpers.getTOEClient().sendTOEMsg(
					req,
					'info',
					'starting to activate the following requests:  ' + a.join(', '),
					err => cb1(err, a)
				),
				(a, cb1) => dsoTasks.activate(
					trace,
					oCP.client1,
					oCP.client2,
					oCP.schema,
					req.body.parameterValues.dataStoreName,
					a,
					cb1
				),
				(o, cb1) => dsoTasks.getOperationInfo(
					trace, oCP.client1, oCP.schema, req.body.parameterValues.dataStoreName,
					null, null, null, {
						requests: [o.operationId]
					}, null, cb1
				),
				(o, cb1) => {
					if (!o) {
						return cb1(new Error('Failed to retrieve status of operation'));
					}
					if (!o.result) {
						return cb1(new Error('Invalid response of dataStore engine: No Result'));
					}
					if (o.result.length !== 1) {
						return cb1(new Error('Invalid response of dataStore engine: Empty result'));
					}
					if (!o.result[0].status) {
						return cb1(new Error('Invalid response of dataStore engine: Missing status'));
					}
					if (o.result[0].status !== 'FINISHED') {
						return cb1(
							new Error(
								'Operation ' + o.result[0].operationId + ' ended with status: ' + o.result[0].status
							)
						);
					}
					return cb1(null, o);
				},
				(o, cb1) => helpers.getTOEClient().sendTOEMsg(
					req,
					'info',
					'all requests activated with operation: ' + o.operationId + ' using activation id ' + o.activationId,
					cb1
				)
			],
			helpers.dbClient.closeDBClientPair.bind(this, oCP)
		);
	},
	loadFile: function(req, res) {
		var oCP = null;
		helpers.getTOEClient().executeTask(
			req, res, 'loadFile', [
				helpers.getTOEClient().sendTOEMsg.bind(
					null,
					req,
					'info',
					'Loading csv data from file ' +
					req.body.parameterValues.fileName +
					' to ' +
					req.body.parameterValues.dataStoreName +
					'/' +
					req.body.parameterValues.inboundTable
				),
				helpers.dbClient.createDBClientPair,
				function(oClientPair, cb1) {
					oCP = oClientPair;
					return fs.readFile(
						req.body.parameterValues.fileName, {
							encoding: 'utf8'
						},
						cb1
					);
				},
				function(data, cb1) {
					dsoTasks.storeCSV(
						trace,
						oCP.client1,
						oCP.client2,
						oCP.schema,
						req.body.parameterValues.dataStoreName,
						req.body.parameterValues.inboundTable,
						data,
						req.body.parameterValues.withHeaderLine,
						cb1
					);
				},
				(o, cb1) => dsoTasks.getOperationInfo(
					trace, oCP.client1, oCP.schema, req.body.parameterValues.dataStoreName,
					null, null, null, {
						requests: [o.operationId]
					}, null, cb1
				),
				(o, cb1) => {
					if (!o) {
						return cb1(new Error('Failed to retrieve status of operation'));
					}
					if (!o.result) {
						return cb1(new Error('Invalid response of dataStore engine: No Result'));
					}
					if (o.result.length !== 1) {
						return cb1(new Error('Invalid response of dataStore engine: Empty result'));
					}
					if (!o.result[0].status) {
						return cb1(new Error('Invalid response of dataStore engine: Missing status'));
					}
					if (o.result[0].status !== 'FINISHED') {
						return cb1(
							new Error(
								'Operation ' + o.result[0].operationId + ' ended with status: ' + o.result[0].status
							)
						);
					}
					return cb1(null);
				}
			],
			helpers.dbClient.closeDBClientPair.bind(this, oCP)
		);
	},
	loadHTTP: function(req, res) {
		var oCP = null;
		helpers.getTOEClient().executeTask(
			req, res, 'loadHTT', [
				helpers.getTOEClient().sendTOEMsg.bind(
					null, req, 'info',
					'Loading csv data from: ' +
					req.body.parameterValues.url +
					' to ' +
					req.body.parameterValues.dataStoreName +
					'/' +
					req.body.parameterValues.inboundTable
				),
				helpers.dbClient.createDBClientPair,
				(oClientPair, cb1) => {
					oCP = oClientPair;
					return cb1(null);
				},
				(cb1) => {
					var requestObj = {
						url: req.body.parameterValues.url,
						timeout: 20 * 1000
					};
					if (req.body.parameterValues.proxy) {
						requestObj.proxy = req.body.parameterValues.proxy;
					}
					if (req.body.parameterValues.timeout) {
						requestObj.timeout = parseInt(req.body.parameterValues.timeout);
					}
					return request(requestObj, cb1);
				},
				(response, body, cb1) => cb1(
					response.statusCode < 200 || response.statusCode > 299 ? new helpers.Messages([{
						severity: 'error',
						msg: 'Could not retrieve ' + req.body.parameterValues.url
					}, {
						severity: 'error',
						msg: 'retrieved status code: ' + response.statusCode
					}, {
						severity: 'error',
						msg: response.statusMessage
					}]) : null, body),
				(body, cb1) => dsoTasks.storeCSV(
					trace,
					oCP.client1,
					oCP.client2,
					oCP.schema,
					req.body.parameterValues.dataStoreName,
					req.body.parameterValues.inboundTable,
					body,
					req.body.parameterValues.withHeaderLine,
					cb1
				),
				(o, cb1) => dsoTasks.getOperationInfo(
					trace, oCP.client1, oCP.schema, req.body.parameterValues.dataStoreName,
					null, null, null, {
						requests: [o.operationId]
					}, null, cb1
				),
				(o, cb1) => {
					if (!o) {
						return cb1(new Error('Failed to retrieve status of operation'));
					}
					if (!o.result) {
						return cb1(new Error('Invalid response of dataStore engine: No Result'));
					}
					if (o.result.length !== 1) {
						return cb1(new Error('Invalid response of dataStore engine: Empty result'));
					}
					if (!o.result[0].status) {
						return cb1(new Error('Invalid response of dataStore engine: Missing status'));
					}
					if (o.result[0].status !== 'FINISHED') {
						return cb1(
							new Error(
								'Operation ' + o.result[0].operationId + ' ended with status: ' + o.result[0].status
							)
						);
					}
					return cb1(null);
				}
			],
			helpers.dbClient.closeDBClientPair.bind(this, oCP)
		);
	},
	loadSQL: function(req, res) {
		var oCP = null;
		helpers.getTOEClient().executeTask(
			req, res, 'loadSQL', [
				helpers.getTOEClient().sendTOEMsg.bind(
					null, req, 'info',
					'Loading sql from: ' +
					req.body.parameterValues.sql +
					' to ' +
					req.body.parameterValues.dataStoreName +
					'/' +
					req.body.parameterValues.inboundTable
				),
				helpers.dbClient.createDBClientPair,
				(oClientPair, cb1) => {
					oCP = oClientPair;
					cb1(null);
				},
				cb1 => dsoTasks.storeSQL(
					trace,
					oCP.client1,
					oCP.client2,
					oCP.schema,
					req.body.parameterValues.dataStoreName,
					req.body.parameterValues.inboundTable,
					req.body.parameterValues.sql,
					cb1
				),
				(o, cb1) => dsoTasks.getOperationInfo(
					trace, oCP.client1, oCP.schema, req.body.parameterValues.dataStoreName,
					null, null, null, {
						requests: [o.operationId]
					}, null, cb1
				),
				(o, cb1) => {
					if (!o) {
						return cb1(new Error('Failed to retrieve status of operation'));
					}
					if (!o.result) {
						return cb1(new Error('Invalid response of dataStore engine: No Result'));
					}
					if (o.result.length !== 1) {
						return cb1(new Error('Invalid response of dataStore engine: Empty result'));
					}
					if (!o.result[0].status) {
						return cb1(new Error('Invalid response of dataStore engine: Missing status'));
					}
					if (o.result[0].status !== 'FINISHED') {
						return cb1(
							new Error(
								'Operation ' + o.result[0].operationId + ' ended with status: ' + o.result[0].status
							)
						);
					}
					return cb1(null);
				}
			],
			helpers.dbClient.closeDBClientPair.bind(this, oCP)
		);
	},
	executeFlowGraph: function(req, res) {
		var oCP = null;
		helpers.getTOEClient().executeTask(
			req, res, 'executeFlowGraph', [
				cb1 => helpers.getTOEClient().sendTOEMsg(
					req, 'info', 'execute flowgraph: ' + req.body.parameterValues.flowGraphName, cb1
				),
				helpers.dbClient.createDBClientPair,
				(oClientPair, cb1) => {
					oCP = oClientPair;
					return cb1(null);
				},
				cb1 => fgTasks.execute(
					trace, oCP.client1, oCP.schema, req.body.parameterValues.flowGraphName, cb1
				)
			],
			helpers.dbClient.closeDBClientPair.bind(this, oCP)
		);
	}
};