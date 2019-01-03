/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var _ = require('lodash');
var hdbext = require('@sap/hdbext');
var getMetadata = require('./getMetadata');
var getNewId = require('./getNewId');
var updateOperationStatus = require('./updateOperationStatus');
var setOperationDetails = require('./setOperationDetails');
var writeMessage = require('./writeMessage');

exports.doIt = function(oTrace, oClient, oClient2Connection, sSchema, sNameDSO, cb) {
	var operationId = null;
	var aFA = null;
	var sSql = null;

	oTrace.info('call of "repairRunningOperations( ' + sSchema + ', ' + sNameDSO + ' )"');

	if (!sSchema) {
		cb(new Error('No Schema provided'));
		return;
	}
	if (!sNameDSO) {
		cb(new Error('No DataStore provided'));
		return;
	}

	var operationIdColName = '"' + 'technicalKey.operationId' + '"';
	var statusColName = '"' + 'technicalAttributes.status' + '"';
	var startTimestampColName = '"' + 'technicalAttributes.startTimestamp' + '"';
	var sRunningOperationsTabname = '';
	var sOperationHistoryTabname = '';

	// use constants from annotations
	var sOperation = 'REPAIR_RUNNING_OP';
	var sStatusRunning = 'RUNNING';
	var sStatusFinished = 'FINISHED';
	var sStatusFailed = 'FAILED';

	var oMsgStart = {
		'type': 'I',
		'number': 13001,
		'text': 'Start repair of running-operations'
	};
	var oMsgFailed = {
		'type': 'E',
		'number': 13003,
		'text': 'Repair of running-operations failed'
	};

	function MsgSuccessDetail(sText) {
		var oMe = this;
		oMe.type = 'I';
		oMe.number = 99998;
		oMe.text = sText;
	}

	function MsgErrorDetail(sText) {
		var oMe = this;
		oMe.type = 'E';
		oMe.number = 99999;
		oMe.text = sText;
	}

	var oMetadata = {};

	async.waterfall(
		[
			cb1 => getMetadata.doIt(oTrace, oClient, sSchema, sNameDSO, cb1),
			(oMD, cb1) => {
				oMetadata = oMD;
				sRunningOperationsTabname = [
					'"',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
					'"."',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
					'.',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(oMetadata.runningOperations.name),
					'"'
				].join('');
				sOperationHistoryTabname = [
					'"',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
					'"."',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
					'.',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(oMetadata.operationHistory.name),
					'"'
				].join('');
				return cb1();
			},
			cb1 => getNewId.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				'OPERATION_REQUEST', sOperation, sStatusRunning, false /*withCommit*/ , cb1
			),
			(opID, cb1) => {
				operationId = opID;
				return writeMessage.doIt(
					oTrace, oClient2Connection, sSchema, sNameDSO, oMetadata,
					operationId, [oMsgStart], true /*withCommit*/ , cb1
				);
			},
			cb1 => oClient.commit(cb1),
			cb1 => updateOperationStatus.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				operationId, sStatusFinished, false /*withCommit*/ , cb1
			),
			// Get all operations which are currently running.
			// Ignore those, which are started in the last 10 seconds.
			// This delay is needed, because after the 'insert'' of the entry into the table
			// 'runningOperations' the lock is released (via commit) and then a new lock
			// is acquired for the 'delete'. Without this delay an operation might get selected
			// which is currently between these two locks
			cb1 => oClient.prepare(
				'select ' + operationIdColName + ' as "id" ' +
				'  from ' + sOperationHistoryTabname +
				'  where ' + statusColName + ' = \'RUNNING\'' +
				'    and ' + startTimestampColName + ' <= ADD_SECONDS( CURRENT_TIMESTAMP, -10 )',
				cb1
			),
			(statement, cb1) => statement.exec([], cb1),
			(aRunningOperations, cb1) => async.mapSeries(
				aRunningOperations,
				(runningOperation, cb2) => async.waterfall(
					[
						cb3 => oClient.prepare(
							'select * from ' + sRunningOperationsTabname +
							' where ' + operationIdColName + ' = ' + parseInt(runningOperation.id) +
							' for update nowait ',
							cb3
						),
						(statement, cb3) => statement.exec([], cb3),
						(rows, cb3) => cb3(null)
					],
					err => {
						if (err) {
							if (err.code === 146) {
								// "Resource busy and acquire with NOWAIT specified"
								// Operation still running => ok
								// Use operationId '-1' as indicator, that no action is needed.
								cb2(null, -1);
								return;
							} else {
								cb2(err);
								return;
							}
						}
						// Operations stored with status 'Running', but there is
						// no locked entry in table runningOperations
						return cb2(null, runningOperation.id);
					}
				),
				(err, aOperations) => {
					if (err) {
						cb1(err, operationId);
						return;
					}
					var aFailedOperations = _.filter(
						aOperations,
						function(id) {
							return id !== -1 && id !== operationId;
						});
					cb1(null, aFailedOperations);
				}),
			(aFailedOperations, cb1) => {
				aFA = aFailedOperations;
				if (!aFailedOperations || aFailedOperations.length === 0) {
					return cb1(null);
				}
				return setOperationDetails.doIt(
					oTrace, oClient, sSchema, sNameDSO, oMetadata,
					operationId, {
						'failedOperations': aFailedOperations
					}, false /*withCommit*/ ,
					cb1
				);
			},
			cb1 => {
				if (!aFA || aFA.length === 0) {
					return cb1(null);
				}
				sSql =
					'update ' + sOperationHistoryTabname +
					' set ' + statusColName + ' = \'FAILED\'' +
					' where ' + operationIdColName + ' = ?';
				return async.waterfall(
					[
						cb2 => oClient.prepare(sSql, cb2),
						(statement, cb2) => statement.exec(aFA.map(n => [n]), cb2),
						(o, cb2) => cb2()
					],
					cb1
				);
			},
			cb1 => {
				// delete all entries in table 'runningOperations' for which
				// there is no 'running' operation in table 'operationHistory'
				sSql =
					'delete from ' + sRunningOperationsTabname + ' RUN_OP' +
					'  where not exists ( ' +
					'    select ' + operationIdColName +
					'      from ' + sOperationHistoryTabname + ' OPEN_HIST' +
					'      where RUN_OP.' + operationIdColName +
					'          = OPEN_HIST.' + operationIdColName +
					'        and OPEN_HIST.' + statusColName + ' = \'RUNNING\' )';

				return oClient.prepare(sSql, cb1);
			},
			(statement, cb1) => statement.exec([], cb1),
			(o, cb1) => writeMessage.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				operationId, [
					new MsgSuccessDetail(JSON.stringify(aFA))
				], false /*withCommit*/ , cb1
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
					aMessages.push(new MsgErrorDetail('SQL: ' + sSql));
					aMessages.push(oMsgFailed);
					return writeMessage.doIt(
						oTrace, oClient2Connection, sSchema, sNameDSO, oMetadata,
						operationId, aMessages, true /*withCommit*/ , cb1
					);
				},
				cb1 => oClient.rollback(cb1),
				cb1 => updateOperationStatus.doIt(
					oTrace, oClient2Connection, sSchema, sNameDSO, oMetadata,
					operationId, sStatusFailed, true /*withCommit*/ , cb1
				)
			],
			err1 => err1 ? cb(err1) : cb(
				null, {
					'operationId': operationId
				}
			)
		) : cb(
			null, {
				'operationId': operationId
			}
		)
	);
};