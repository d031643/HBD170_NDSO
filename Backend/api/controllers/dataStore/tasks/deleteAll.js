/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var hdbext = require('@sap/hdbext');
var _ = require('lodash');
var getMetadata = require('./getMetadata');
var getNewId = require('./getNewId');
var updateOperationStatus = require('./updateOperationStatus');
var writeMessage = require('./writeMessage');
var getSubscribers = require('./getSubscribers');

exports.doIt = function(oTrace, oClient, oClient2Connection, sSchema, sNameDSO, cb) {
	var operationId = null;

	oTrace.info('call of "deleteAll( ' + sSchema + ', ' + sNameDSO + ' )"');

	if (!sSchema) {
		return cb(new Error('No Schema provided'));
	}
	if (!sNameDSO) {
		return cb(new Error('No DataStore provided'));
	}

	// use constants from annotations
	var sOperationDelete = 'DELETE_ALL';
	var sStatusRunning = 'RUNNING';
	var sStatusFinished = 'FINISHED';
	var sStatusFailed = 'FAILED';

	var oMsgStart = {
		'type': 'I',
		'number': 9001,
		'text': 'Start delete-all'
	};
	var oMsgSuccess = {
		'type': 'S',
		'number': 9002,
		'text': 'Delete-all finished successfully.'
	};
	var oMsgFailed = {
		'type': 'E',
		'number': 9003,
		'text': 'Delete-all failed'
	};
	var oMsgSubscriberExist = {
		'type': 'E',
		'number': 9004,
		'text': 'Please delete subscribers first'
	};

	function MsgErrorDetail(sText) {
		var oMe = this;
		oMe.type = 'E';
		oMe.number = 99999;
		oMe.text = sText;
	}

	var oMetadata = {};

	return async.waterfall(
		[
			cb1 => getMetadata.doIt(oTrace, oClient, sSchema, sNameDSO, cb1),
			(oMD, cb1) => {
				oMetadata = oMD;
				return getNewId.doIt(
					oTrace, oClient, sSchema, sNameDSO, oMetadata,
					'OPERATION_REQUEST', sOperationDelete, sStatusRunning, false /*withCommit*/ , cb1
				);
			},
			(opId, cb1) => {
				operationId = opId;
				return writeMessage.doIt(
					oTrace, oClient2Connection, sSchema, sNameDSO, oMetadata,
					operationId, [oMsgStart], true /*withCommit*/ , cb1
				);
			},
			cb1 => oClient.commit(cb1),
			// deleteAll is only allowed if there are no subscribers registered
			cb1 => getSubscribers.doIt(oTrace, oClient, sSchema, sNameDSO, cb1),
			(oResult, cb1) => oResult && oResult.result.length !== 0 ? cb1(
				new Error(oMsgSubscriberExist.text)
			) : cb1(null),
			cb1 => updateOperationStatus.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				operationId, sStatusFinished, false /*withCommit*/ , cb1
			),
			cb1 => {
				var aTables = _.map(oMetadata.activationQueues, function(queue) {
					return {
						'name': queue.name,
						'where': ''
					};
				});
				if (oMetadata.changeLog) {
					aTables.push({
						'name': oMetadata.changeLog.name,
						'where': ''
					});
				}
				aTables.push({
					'name': oMetadata.activeData.name,
					'where': ''
				});
				aTables.push({
					'name': oMetadata.subscribers.name,
					'where': ''
				});
				aTables.push({
					'name': oMetadata.aggregationHistory.name,
					'where': ''
				});

				var sWhere = ' where "technicalKey.operationId" != ?';
				aTables.push({
					'name': oMetadata.operationHistory.name,
					'where': sWhere
				});
				aTables.push({
					'name': oMetadata.logMessages.name,
					'where': sWhere
				});
				aTables.push({
					'name': oMetadata.affectedRequests.name,
					'where': sWhere
				});
				aTables.push({
					'name': oMetadata.runningOperations.name,
					'where': sWhere
				});
				aTables.push({
					'name': oMetadata.idGenerator.name,
					'where': ' where "technicalKey.id" != ?'
				});

				return async.mapSeries(
					aTables,
					(table, cb2) => async.waterfall(
						[
							cb3 => {
								var sTabName = [
									'"',
									hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
									'"."',
									hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
									'.',
									hdbext.sqlInjectionUtils.escapeDoubleQuotes(table.name),
									'"'
								].join('');
								return oClient.prepare('delete from ' + sTabName + table.where, cb3);
							},
							(statement, cb3) => statement.exec([operationId], cb3)
						],
						err1 => cb2(err1)
					),
					err => cb1(err)
				);
			},
			cb1 => writeMessage.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				operationId, [oMsgSuccess], false /*withCommit*/ , cb1
			),
			cb1 => oClient.commit(cb1)
		],
		err => err ? async.waterfall(
			[
				cb1 => {
					var aMessages = [];
					if (err.aNdsoErrorDetails && err.aNdsoErrorDetails.length !== 0) {
						aMessages = _.flatten(_.map(err.aNdsoErrorDetails, function(oDetail) {
							return _.map(oDetail, oV => new MsgErrorDetail(oV.toString()));
						}));
					}
					if (err.message) {
						aMessages.push(new MsgErrorDetail(err.message));
					}
					if (err.stack) {
						aMessages.push(new MsgErrorDetail(err.stack));
					}
					aMessages.push(oMsgFailed);
					return writeMessage.doIt(
						oTrace, oClient2Connection, sSchema, sNameDSO, oMetadata,
						operationId, aMessages, true /*withCommit*/ , cb1);
				},
				cb1 => oClient.rollback(cb1),
				cb1 => updateOperationStatus.doIt(
					oTrace, oClient2Connection, sSchema, sNameDSO, oMetadata,
					operationId, sStatusFailed, true /*withCommit*/ , cb1
				)
			],
			err1 => err1 ? cb(err1) : cb(null, {
				'operationId': operationId
			})
		) : cb(null, {
			'operationId': operationId
		})
	);
};