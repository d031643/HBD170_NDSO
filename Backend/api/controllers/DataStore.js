/* Copyright (c) 2016 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';
var async = require('async');
var helpers = require('../helpers');
var dsoTasks = require('./dataStore/datastoreTasks');
var util = require('util');
var trace = helpers.trace;

function handleResponse(res, err, o) {
	if (err) {
		res.status(500);
		res.send({
			message: err.message,
			stack: err.stack
		});
	} else {
		res.send(o);
	}
}

var cbTask = function(err, oTaskObj, op, cb) {
	if (err) {
		trace.error(op + ' -- failure');
		trace.error(util.inspect(err, {
			'showhidden': false,
			'depth': null
		}));
		return cb(new Error('EXECUTION_ERROR', [op + ': ' + err.message]));
	} else {
		return cb(null, oTaskObj);
	}
};

/*************************************DELETE API's */
function dataStoreDeleteAll(oClient, oClient2, oInput, cb) {
	var op = 'dataStoreDeleteAll';
	dsoTasks.deleteAll(
		trace,
		oClient,
		oClient2,
		oInput.schemaName,
		oInput.dataStoreName,
		function(err, oObj) {
			cbTask(err, oObj, op, cb);
		}
	);
}

function dataStoreCleanupMetadata(oClient, oClient2, oInput, cb) {
	var op = 'dataStoreCleanupMetadata';
	dsoTasks.cleanupMetadata(
		trace,
		oClient,
		oClient2,
		oInput.schemaName,
		oInput.dataStoreName,
		oInput.maxRequestId,
		oInput.maxTimestamp,
		function(err, oObj) {
			cbTask(err, oObj, op, cb);
		}
	);
}

function dataStoreCleanupChangelog(oClient, oClient2, oInput, cb) {
	var op = 'dataStoreCleanupChangelog';
	dsoTasks.cleanupChangelog(
		trace,
		oClient,
		oClient2,
		oInput.schemaName,
		oInput.dataStoreName,
		oInput.requestIds,
		function(err, oObj) {
			cbTask(err, oObj, op, cb);
		}
	);
}

function dataStoreRollback(oClient, oClient2, oInput, cb) {
	var op = 'dataStoreRollback';
	dsoTasks.rollback(
		trace,
		oClient,
		oClient2,
		oInput.schemaName,
		oInput.dataStoreName,
		oInput.activationIds,
		function(err, oObj) {
			cbTask(err, oObj, op, cb);
		}
	);
}

function dataStoreDeleteLoads(oClient, oClient2, oInput, cb) {
	var op = 'dataStoreDeleteLoads';
	dsoTasks.deleteRequest(
		trace,
		oClient,
		oClient2,
		oInput.schemaName,
		oInput.dataStoreName,
		oInput.requestIds,
		function(err, oObj) {
			cbTask(err, oObj, op, cb);
		}
	);
}

function dataStoreDeleteWithFilter(oClient, oClient2, oInput, cb) {
	var op = 'dataStoreDeleteWithFilter';
	dsoTasks.deleteWithFilter(
		trace,
		oClient,
		oClient2,
		oInput.schemaName,
		oInput.dataStoreName,
		oInput.sWhere,
		oInput.propagateDeletion,
		function(err, oObj) {
			cbTask(err, oObj, op, cb);
		}
	);
}

function dataStoreRemoveSubscriber(oClient, oClient2, oInput, cb) {
	var op = 'dataStoreRemoveSubscriber';
	trace.info(op + '  -- start');
	dsoTasks.removeSubscriber(
		trace, oClient, oClient2, oInput.schemaName, oInput.dataStoreName,
		oInput.subscriberName,
		function(err, oObj) {
			cbTask(err, oObj, op, cb);
		}
	);
}
/**************************************CHANGING API's */
function dataStoreRepairRunningOperations(oClient, oClient2, oInput, cb) {
	var op = 'dataStoreRepairRunningOperations';
	trace.info(op + '  -- start');
	dsoTasks.repairRunningOperations(
		trace,
		oClient,
		oClient2,
		oInput.schemaName,
		oInput.dataStoreName,
		function(err, oObj) {
			cbTask(err, oObj, op, cb);
		}
	);
}

function dataStoreUploadData(oClient, oClient2, oInput, cb) {
	var op = 'dataStoreUploadData';
	trace.info(op + '  -- start');
	dsoTasks.storeCSV(
		trace,
		oClient,
		oClient2,
		oInput.schemaName,
		oInput.dataStoreName,
		oInput.inboundQueueName,
		oInput.data,
		false,
		function(err, oObj) {
			cbTask(err, oObj, op, cb);
		}
	);
}

function dataStoreStoreSQL(oClient, oClient2, oInput, cb) {
	var op = 'dataStoreStoreSQL';
	trace.info(op + '  -- start');
	dsoTasks.storeSQL(
		trace,
		oClient,
		oClient2,
		oInput.schemaName,
		oInput.dataStoreName,
		oInput.inboundQueueName,
		oInput.sql,
		function(err, oObj) {
			cbTask(err, oObj, op, cb);
		}
	);
}

function dataStoreActivateLoads(oClient, oClient2, oInput, cb) {
	var op = 'dataStoreActivateLoads';
	trace.info(op + '  -- start');
	dsoTasks.activate(
		trace,
		oClient,
		oClient2,
		oInput.schemaName,
		oInput.dataStoreName,
		oInput.requestIds,
		function(err, oObj) {
			cbTask(err, oObj, op, cb);
		}
	);
}

function dataStoreAddSubscriber(oClient, oClient2, oInput, cb) {
	var op = 'dataStoreAddSubscriber';
	trace.info(op + '  -- start');
	dsoTasks.addSubscriber(
		trace,
		oClient,
		oClient2,
		oInput.schemaName,
		oInput.dataStoreName,
		oInput.subscriberName,
		oInput.subscriberDescription,
		function(err, oObj) {
			cbTask(err, oObj, op, cb);
		}
	);
}

function dataStoreResetSubscriber(oClient, oClient2, oInput, cb) {
	var op = 'dataStoreResetSubscriber';
	trace.info(op + '  -- start');
	dsoTasks.resetSubscriber(
		trace, oClient, oClient2, oInput.schemaName, oInput.dataStoreName,
		oInput.subscriberName,
		function(err, oObj) {
			cbTask(err, oObj, op, cb);
		}
	);
}
/********************************************************************Getter API's */
function dataStoreCheckMetadataConsistency(oClient, oClient2, oInput, cb) {
	var op = 'dataStoreCheckMetadataConsistency';
	trace.info(op + '  -- start');
	dsoTasks.checkMetadataConsistency(
		trace,
		oClient,
		oClient2,
		oInput.schemaName,
		oInput.dataStoreName,
		function(err, oObj) {
			cbTask(err, oObj, op, cb);
		}
	);
}

function dataStoreGetRequestsForActivation(oClient, oInput, cb) {
	var op = 'dataStoreGetRequestsForActivation';
	trace.info(op + '  -- start');
	dsoTasks.getRequestsForActivation(
		trace,
		oClient,
		oInput.schemaName,
		oInput.dataStoreName,
		oInput.maxRequestId,
		function(err, oObj) {
			cbTask(err, oObj, op, cb);
		}
	);
}

function dataStoreGetRequestsForRollback(oClient, oInput, cb) {
	var op = 'dataStoreGetRequestsForRollback';
	trace.info(op + '  -- start');
	dsoTasks.getRequestsForRollback(
		trace,
		oClient,
		oInput.schemaName,
		oInput.dataStoreName,
		oInput.minRequestId,
		function(err, oObj) {
			cbTask(err, oObj, op, cb);
		}
	);
}

function dataStoreGetRequestsForDeletion(oClient, oInput, cb) {
	var op = 'dataStoreGetRequestsForDeletion';
	trace.info(op + '  -- start');
	dsoTasks.getRequestsForDeletion(
		trace,
		oClient,
		oInput.schemaName,
		oInput.dataStoreName,
		function(err, oObj) {
			cbTask(err, oObj, op, cb);
		}
	);
}

function dataStoreGetRequestsForCleanup(oClient, oInput, cb) {
	var op = 'dataStoreGetRequestsForCleanup';
	trace.info(op + '  -- start');
	dsoTasks.getRequestsForCleanup(
		trace,
		oClient,
		oInput.schemaName,
		oInput.dataStoreName,
		oInput.maxRequestId,
		oInput.maxTimestamp,
		function(err, oObj) {
			cbTask(err, oObj, op, cb);
		}
	);
}

function dataStoreGetOperationsForRequest(oClient, oInput, cb) {
	var op = 'dataStoreGetOperationsForRequest';
	trace.info(op + '  -- start');
	dsoTasks.getOperationsForRequest(
		trace,
		oClient,
		oInput.schemaName,
		oInput.dataStoreName,
		oInput.requestId,
		function(err, oObj) {
			cbTask(err, oObj, op, cb);
		}
	);
}

function dataStoreGetOperationInfo(oClient, oInput, cb) {
	var op = 'dataStoreGetOperationInfo';
	trace.info(op + '  -- start');
	dsoTasks.getOperationInfo(
		trace,
		oClient,
		oInput.schemaName,
		oInput.dataStoreName,
		oInput.filter.oOperationFilter,
		oInput.filter.oStatusFilter,
		oInput.filter.oTimeFilter,
		oInput.filter.oOperationIdFilter,
		oInput.filter.oUserFilter,
		function(err, oObj) {
			cbTask(err, oObj, op, cb);
		}
	);
}

function dataStoreGetRowCountWithFilter(oClient, oClient2, oInput, cb) {
	var op = 'dataStoreGetRowCountWithFilter';
	trace.info(op + '  -- start');
	dsoTasks.getRowcountWithFilter(
		trace,
		oClient,
		oClient2,
		oInput.schemaName,
		oInput.dataStoreName,
		oInput.sWhere,
		function(err, oObj) {
			cbTask(err, oObj, op, cb);
		}
	);
}

function dataStoreGetLogForOperation(oClient, oInput, cb) {
	var op = 'dataStoreGetLogForOperation';
	trace.info(op + '  -- start');
	dsoTasks.getLogForOperation(
		trace,
		oClient,
		oInput.schemaName,
		oInput.dataStoreName,
		oInput.operationId,
		function(err, oObj) {
			cbTask(err, oObj, op, cb);
		}
	);
}

function dataStoreGetSubscribers(oClient, oInput, cb) {
	var op = 'dataStoreGetSubscribers';
	trace.info(op + '  -- start');
	dsoTasks.getSubscribers(
		trace,
		oClient,
		oInput.schemaName,
		oInput.dataStoreName,
		function(err, oObj) {
			cbTask(err, oObj, op, cb);
		}
	);
}

module.exports = {
	getDataStores: function(req, res) {
		var oCP = null;
		async.waterfall(
			[
				helpers.dbClient.createDBClientPair,
				(oClientPair, cb1) => {
					oCP = oClientPair;
					dsoTasks.getDataStores(
						trace,
						oCP.client1,
						oCP.schema,
						cb1
					);
				},
				(oDataStores, cb1) => cb1(
					undefined, {
						list: oDataStores.result.map(
							ds => {
								var a1 = ds.match('^.*\\.(.*)$');
								var s = a1 ? a1[1] : ds;
								return {
									name: ds,
									text: s
								};
							}
						)
					}
				)
			],
			(err, o) => {
				helpers.dbClient.closeDBClientPair(oCP);
				handleResponse(res, err, o);
			}
		);
	},
	deleteAll: function(req, res) {
		var oCP = null;
		async.waterfall(
			[
				helpers.dbClient.createDBClientPair,
				(oClientPair, cb1) => {
					oCP = oClientPair;
					return dataStoreDeleteAll(
						trace,
						oCP.client1,
						oCP.client2, {
							schemaName: oCP.schemaName,
							dataStoreName: req.swagger.params.dataStoreName.value
						},
						cb1
					);
				}
			],
			function(err, o) {
				helpers.dbClient.closeDBClientPair(oCP);
				handleResponse(res, err, o);
			}
		);
	},
	deleteWithFilter: function(req, res) {
		var oCP = null;
		async.waterfall(
			[
				helpers.dbClient.createDBClientPair,
				(oClientPair, cb1) => {
					oCP = oClientPair;
					cb1(null);
				},
				cb1 => dataStoreDeleteWithFilter(
					trace,
					oCP.client1,
					oCP.lient2, {
						schemaName: oCP.schema,
						dataStoreName: req.swagger.params.dataStoreName.value,
						sWhere: req.body.sWhere,
						propagateDeletion: req.query.propagateDeletion
					},
					cb1
				)
			],
			(err, o) => {
				helpers.dbClient.closeDBClientPair(oCP);
				handleResponse(res, err, o);
			}
		);
	},
	getRowCountWithFilter: function(req, res) {
		var oCP = null;
		async.waterfall(
			[
				helpers.dbClient.createDBClientPair,
				(oClientPair, cb1) => {
					oCP = oClientPair;
					return cb1(null);
				},
				cb1 => dataStoreGetRowCountWithFilter(
					trace,
					oCP.client1,
					oCP.client2, {
						schemaName: oCP.schema,
						dataStoreName: req.swagger.params.dataStoreName.value,
						sWhere: decodeURIComponent(req.query.Where),
						propagateDeletion: req.query.propagateDeletion
					},
					cb1
				)
			],
			(err, o) => {
				helpers.dbClient.closeDBClientPair(oCP);
				handleResponse(res, err, o);
			}
		);
	},
	addSubscriber: function(req, res) {
		var oCP = null;
		async.waterfall(
			[
				helpers.dbClient.createDBClientPair,
				(oClientPair, cb1) => {
					oCP = oClientPair;
					cb1(null);
				},
				cb1 => dataStoreAddSubscriber(
					trace,
					oCP.client1,
					oCP.client2, {
						schemaName: oCP.schema,
						dataStoreName: req.swagger.params.dataStoreName.value,
						subscriberName: req.query.subscriberName,
						subscriberDescription: req.query.subscriberDescription
					},
					cb1
				)
			],
			(err, o) => {
				helpers.dbClient.closeDBClientPair(oCP);
				handleResponse(res, err, o);
			}
		);
	},
	removeSubscriber: function(req, res) {
		var oCP = null;
		async.waterfall(
			[
				helpers.dbClient.createDBClientPair,
				(oClientPair, cb1) => {
					oCP = oClientPair;
					cb1(null);
				},
				cb1 => dataStoreRemoveSubscriber(
					trace,
					oCP.client1,
					oCP.client2, {
						schemaName: oCP.schema,
						dataStoreName: req.swagger.params.dataStoreName.value,
						subscriberName: req.query.subscriberName
					},
					cb1
				)
			],
			(err, o) => {
				helpers.dbClient.closeDBClientPair(oCP);
				handleResponse(res, err, o);
			});
	},
	resetSubscriber: function(req, res) {
		var oCP = null;
		async.waterfall(
			[
				helpers.dbClient.createDBClientPair,
				function(oClientPair, cb1) {
					oCP = oClientPair;
					cb1(null);
				},
				cb1 => dataStoreResetSubscriber(
					trace,
					oCP.client1,
					oCP.lient2, {
						schemaName: oCP.schema,
						dataStoreName: req.swagger.params.dataStoreName.value,
						subscriberName: req.query.subscriberName
					},
					cb1
				)
			],
			(err, o) => {
				helpers.dbClient.closeDBClientPair(oCP);
				handleResponse(res, err, o);
			}
		);
	},
	getSubscribers: function(req, res) {
		var oCP = null;
		async.waterfall(
			[
				helpers.dbClient.createDBClientPair,
				(oClientPair, cb1) => {
					oCP = oClientPair;
					return cb1(null);
				},
				cb1 => dataStoreGetSubscribers(
					trace,
					oCP.client1, {
						schemaName: oCP.schema,
						dataStoreName: req.swagger.params.dataStoreName.value
					},
					cb1
				)
			],
			(err, o) => {
				helpers.dbClient.closeDBClientPair(oCP);
				handleResponse(res, err, o);
			}
		);
	},
	uploadData: function(req, res) {
		var oCP = null;
		async.waterfall(
			[
				helpers.dbClient.createDBClientPair,
				(oClientPair, cb1) => {
					oCP = oClientPair;
					return cb1(null);
				},
				cb1 => dataStoreUploadData(
					trace,
					oCP.client1,
					oCP.client2, {
						schemaName: oCP.schema,
						dataStoreName: req.swagger.params.dataStoreName.value,
						inboundQueueName: req.query.inboundQueueName,
						data: req.body
					},
					cb1
				)
			],
			(err, o) => {
				helpers.dbClient.closeDBClientPair(oCP);
				handleResponse(res, err, o);
			}
		);
	},
	storeSQL: function(req, res) {
		var oCP = null;
		async.waterfall(
			[
				helpers.dbClient.createDBClientPair,
				(oClientPair, cb1) => {
					oCP = oClientPair;
					cb1(null);
				},
				cb1 => dataStoreStoreSQL(
					trace,
					oCP.client1,
					oCP.client2, {
						schemaName: oCP.schema,
						dataStoreName: req.swagger.params.dataStoreName.value,
						inboundQueueName: req.query.inboundQueueName,
						sql: req.body.sql
					},
					cb1
				)
			],
			(err, o) => {
				helpers.dbClient.closeClientPair(oCP);
				handleResponse(res, err, o);
			}
		);
	},
	activateLoads: function(req, res) {
		var oCP = null;
		async.waterfall(
			[
				helpers.dbClient.createDBClientPair,
				(oClientPair, cb1) => {
					oCP = oClientPair;
					return cb1(null);
				},
				cb1 => dataStoreActivateLoads(
					trace,
					oCP.client1,
					oCP.client2, {
						schemaName: oCP.schema,
						dataStoreName: req.swagger.params.dataStoreName.value,
						requestIds: req.body
					},
					cb1
				)
			],
			(err, o) => {
				helpers.dbClient.closeClientPair(oCP);
				handleResponse(res, err, o);
			}
		);
	},
	deleteLoads: function(req, res) {
		var oCP = null;
		async.waterfall(
			[
				helpers.dbClient.createDBClientPair,
				(oClientPair, cb1) => {
					oCP = oClientPair;
					return cb1(null);
				},
				cb1 => dataStoreDeleteLoads(
					trace,
					oCP.client1,
					oCP.client2, {
						schemaName: oCP.schema,
						dataStoreName: req.swagger.params.dataStoreName.value,
						requestIds: req.body
					},
					cb1
				)
			],
			(err, o) => {
				helpers.dbClient.closeClientPair(oCP);
				handleResponse(res, err, o);
			}
		);
	},
	rollback: function(req, res) {
		var oCP = null;
		async.waterfall(
			[
				helpers.dbClient.createDBClientPair,
				(oClientPair, cb1) => {
					oCP = oClientPair;
					cb1(null);
				},
				cb1 => dataStoreRollback(
					trace,
					oCP.client1,
					oCP.client2, {
						schemaName: oCP.schema,
						dataStoreName: req.swagger.params.dataStoreName.value,
						activationIds: req.body
					},
					cb1
				)
			],
			(err, o) => {
				helpers.dbClient.closeDBClientPair(oCP);
				handleResponse(res, err, o);
			}
		);
	},
	getLogForOperation: function(req, res) {
		var oCP = null;
		async.waterfall(
			[
				helpers.dbClient.createDBClientPair,
				(oClientPair, cb1) => {
					oCP = oClientPair;
					cb1(null);
				},
				cb1 => dataStoreGetLogForOperation(
					trace,
					oCP.client1, {
						schemaName: oCP.schema,
						dataStoreName: req.swagger.params.dataStoreName.value,
						operationId: req.query.operationId
					},
					cb1
				)
			],
			(err, o) => {
				helpers.dbClient.closeDBClientPair(oCP);
				handleResponse(res, err, o);
			}
		);
	},
	getOperationInfo: function(req, res) {
		var queryURI2Object = function(string) {
			if (string) {
				return decodeURIComponent(string).split('/');
			}
		};
		var filter = {
			oOperationFilter: {
				operations: queryURI2Object(req.query.operations)
			},
			oStatusFilter: {
				status: queryURI2Object(req.query.status)
			},
			oUserFilter: {
				users: queryURI2Object(req.query.users)
			},
			oTimeFilter: {
				low: req.query.timeLow,
				high: req.query.timeHigh
			},
			oOperationIdFilter: {
				low: req.query.operationIdLow,
				high: req.query.operationIdHigh,
				requests: queryURI2Object(req.query.operationIds)
			}
		};
		//massage objects
		if (filter.oTimeFilter.low === undefined && filter.oTimeFilter.high === undefined) {
			filter.oTimeFilter = null;
		}
		if (filter.oOperationIdFilter.low === undefined &&
			filter.oOperationIdFilter.high === undefined &&
			filter.oOperationIdFilter.requests === undefined
		) {
			filter.oOperationIdFilter = null;
		}
		if (filter.oUserFilter.users === undefined) {
			filter.oUserFilter = null;
		}
		if (filter.oStatusFilter.status === undefined) {
			filter.oStatusFilter = null;
		}
		if (filter.oOperationFilter.operations === undefined) {
			filter.oStatusFilter = null;
		}
		var oCP = null;
		async.waterfall(
			[
				helpers.dbClient.createDBClientPair,
				(oClientPair, cb1) => {
					oCP = oClientPair;
					return cb1(oCP && oCP.schema ? null : new Error('Failed to retrieve client pair'));
				},
				cb1 => dataStoreGetOperationInfo(
					trace,
					oCP.client1, {
						schemaName: oCP.schema,
						dataStoreName: req.swagger.params.dataStoreName.value,
						filter: filter
					},
					cb1
				)
			],
			(err, o) => {
				helpers.dbClient.closeDBClientPair(oCP);
				handleResponse(res, err, o);
			}
		);
	},
	getRequestsForActivation: function(req, res) {
		var oCP = null;
		async.waterfall(
			[
				helpers.dbClient.createDBClientPair,
				(oClientPair, cb1) => {
					oCP = oClientPair;
					return cb1(oCP && oCP.schema ? null : new Error('Failed to retrieve client pair'));
				},
				cb1 => dataStoreGetRequestsForActivation(
					oCP.client1, {
						schemaName: oCP.schema,
						dataStoreName: req.swagger.params.dataStoreName.value,
						maxRequestId: req.query.maxRequestId
					},
					cb1
				)
			],
			(err, o) => {
				helpers.dbClient.closeDBClientPair(oCP);
				handleResponse(res, err, o);
			}
		);
	},
	getRequestsForRollback: function(req, res) {
		var oCP = null;
		async.waterfall(
			[
				helpers.dbClient.createDBClientPair,
				(oClientPair, cb1) => {
					oCP = oClientPair;
					return cb1(oCP && oCP.schema ? null : new Error('Failed to retrieve client pair'));
				},
				cb1 => dataStoreGetRequestsForRollback(
					trace,
					oCP.client1, {
						schemaName: oCP.schema,
						dataStoreName: req.swagger.params.dataStoreName.value,
						minRequestId: req.query.minRequestId
					},
					cb1
				)
			],
			(err, o) => {
				helpers.dbClient.closeDBClientPair(oCP);
				handleResponse(res, err, o);
			}
		);
	},
	getRequestsForDeletion: function(req, res) {
		var oCP = null;
		async.waterfall(
			[
				helpers.dbClient.createDBClientPair,
				(oClientPair, cb1) => {
					oCP = oClientPair;
					return cb1(oCP && oCP.schema ? null : new Error('Failed to retrieve client pair'));
				},
				cb1 => dataStoreGetRequestsForDeletion(
					trace,
					oCP.client1, {
						schemaName: oCP.schema,
						dataStoreName: req.swagger.params.dataStoreName.value
					},
					cb1
				)
			],
			(err, o) => {
				helpers.dbClient.closeDBClientPair(oCP);
				handleResponse(res, err, o);
			}
		);
	},
	getRequestsForCleanup: function(req, res) {
		var oCP = null;
		async.waterfall(
			[
				helpers.dbClient.createDBClientPair,
				(oClientPair, cb1) => {
					oCP = oClientPair;
					return cb1(oCP && oCP.schema ? null : new Error('Failed to retrieve client pair'));
				},
				cb1 => dataStoreGetRequestsForCleanup(
					trace,
					oCP.client1, {
						schemaName: oCP.schema,
						dataStoreName: req.swagger.params.dataStoreName.value,
						maxRequestId: req.query.maxRequestId,
						maxTimestamp: req.query.maxTimestamp
					},
					cb1
				)
			],
			(err, o) => {
				helpers.dbClient.closeDBClientPair(oCP);
				handleResponse(res, err, o);
			}
		);
	},
	cleanupMetadata: function(req, res) {
		var oCP = null;
		async.waterfall(
			[
				helpers.dbClient.createDBClientPair,
				(oClientPair, cb1) => {
					oCP = oClientPair;
					return cb1(oCP && oCP.schema ? null : new Error('Failed to retrieve client pair'));
				},
				cb1 => dataStoreCleanupMetadata(
					trace,
					oCP.client1,
					oCP.lient2, {
						schemaName: oCP.schema,
						dataStoreName: req.swagger.params.dataStoreName.value,
						maxRequestId: req.query.maxRequestId,
						maxTimestamp: req.query.maxTimestamp
					},
					cb1
				)
			],
			(err, o) => {
				helpers.dbClient.closeDBClientPair(oCP);
				handleResponse(res, err, o);
			}
		);
	},
	cleanupChangelog: function(req, res) {
		var oCP = null;
		async.waterfall(
			[
				helpers.dbClient.createDBClientPair,
				(oClientPair, cb1) => {
					oCP = oClientPair;
					return cb1(oCP && oCP.schema ? null : new Error('Failed to retrieve client pair'));
				},
				cb1 => dataStoreCleanupChangelog(
					trace,
					oCP.client1,
					oCP.client2, {
						schemaName: oCP.schema,
						dataStoreName: req.swagger.params.dataStoreName.value,
						aRequestIds: req.body
					},
					cb1
				)
			],
			(err, o) => {
				helpers.dbClient.closeDBClientPair(oCP);
				handleResponse(res, err, o);
			}
		);
	},
	checkMetadataConsistency: function(req, res) {
		var oCP = null;
		async.waterfall(
			[
				helpers.dbClient.createDBClientPair,
				(oClientPair, cb1) => {
					oCP = oClientPair;
					return cb1(oCP && oCP.schema ? null : new Error('Failed to retrieve client pair'));
				},
				cb1 => dataStoreCheckMetadataConsistency(
					trace,
					oCP.client1,
					oCP.client2, {
						schemaName: oCP.schema,
						dataStoreName: req.swagger.params.dataStoreName.value
					},
					cb1
				)
			],
			(err, o) => {
				helpers.dbClient.closeDBClientPair(oCP);
				handleResponse(res, err, o);
			}
		);
	},
	repairRunningOperations: function(req, res) {
		var oCP = null;
		async.waterfall(
			[
				helpers.dbClient.createDBClientPair,
				(oClientPair, cb1) => {
					oCP = oClientPair;
					return cb1(oCP && oCP.schema ? null : new Error('Failed to retrieve client pair'));
				},
				cb1 => dataStoreRepairRunningOperations(
					trace,
					oCP.client1,
					oCP.client2, {
						schemaName: oCP.schema,
						dataStoreName: req.swagger.params.dataStoreName.value
					},
					cb1
				)
			],
			(err, o) => {
				helpers.dbClient.closeDBClientPair(oCP);
				handleResponse(res, err, o);
			}
		);
	},
	getOperationsForRequest: function(req, res) {
		var oCP = null;
		async.waterfall(
			[
				helpers.dbClient.createDBClientPair,
				(oClientPair, cb1) => {
					oCP = oClientPair;
					return cb1(oCP && oCP.schema ? null : new Error('Failed to retrieve client pair'));
				},
				cb1 => dataStoreGetOperationsForRequest(
					trace,
					oCP.client1, {
						schemaName: oCP.schema,
						dataStoreName: req.swagger.params.dataStoreName.value,
						requestId: req.query.requestId
					},
					cb1
				)
			],
			(err, o) => {
				helpers.dbClient.closeDBClientPair(oCP);
				handleResponse(res, err, o);
			}
		);
	},
	getContent: function(req, res) {
		var oCP = null;
		var oMD = null;
		async.waterfall(
			[
				helpers.dbClient.createDBClientPair,
				(oClientPair, cb1) => {
					oCP = oClientPair;
					return cb1(oCP && oCP.schema ? null : new Error('Failed to retrieve client pair'));
				},
				cb1 => dsoTasks.getMetadata(
					trace,
					oCP.client1,
					oCP.schema,
					req.swagger.params.dataStoreName.value,
					cb1
				),
				(oMetaData, cb1) => {
					oMD = oMetaData;
					cb1(null);
				},
				cb1 => dsoTasks.getRequestInfo(
					trace,
					oCP.client1,
					oCP.schema,
					req.swagger.params.dataStoreName.value,
					null, null, null, null, null,
					cb1
				),
				(o, cb1) => {
					o.metadata = oMD;
					return cb1(null, o);
				}
			],
			(err, o) => {
				helpers.dbClient.closeDBClientPair(oCP);
				handleResponse(res, err, o);
			}
		);
	}
};