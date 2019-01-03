/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var _ = require('lodash');
var hdbext = require('@sap/hdbext');
var getMetadata = require('./getMetadata');
var getNewId = require('./getNewId');
var updateOperationStatus = require('./updateOperationStatus');
var writeMessage = require('./writeMessage');
var writeAffectedRequests = require('./writeAffectedRequests');

exports.doIt = function(oTrace, oClient, oClient2Connection, sSchema, sNameDSO,
	sActivationQueue, sSQL, cb) {
	var operationId = null;
	var loadId = null;

	oTrace.info('call of "storeSQL( ' + sSQL + ', ' + sActivationQueue + ', ' + sSchema + ', ' + sNameDSO + ' )"');

	if (!sSchema) {
		return cb(new Error('No Schema provided'));
	}
	if (!sNameDSO) {
		return cb(new Error('No DataStore provided'));
	}
	if (!sActivationQueue) {
		return cb(new Error('No activation queue table provided'));
	}
	if (!sSQL) {
		return cb(new Error('No SQL-string provided'));
	}

	var loadIdColName = '"' + 'technicalKey.loadId' + '"';
	var recordNoColName = '"' + 'technicalKey.recordNo' + '"';

	var sActivationQueueTabName = [
		'"',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
		'"."',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
		'.',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(sActivationQueue),
		'"'
	].join('');

	// use constants from annotations
	var sOperationLoad = 'LOAD';
	var sStatusRunning = 'RUNNING';
	var sStatusFinished = 'FINISHED';
	var sStatusFailed = 'FAILED';

	var oMsgStart = {
		'type': 'I',
		'number': 5001,
		'text': 'Start inserting via SQL'
	};
	var oMsgSuccess = {
		'type': 'S',
		'number': 5002,
		'text': 'Inserting via SQL finished successfully. LoadId = &1, #Lines=&2'
	};
	var oMsgFailed = {
		'type': 'E',
		'number': 5003,
		'text': 'Inserting via SQL failed'
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
			cb1 => getMetadata.doIt(
				oTrace, oClient, sSchema, sNameDSO, cb1
			),
			(oMD, cb1) => {
				oMetadata = oMD;
				return getNewId.doIt(
					oTrace, oClient, sSchema, sNameDSO, oMetadata,
					'OPERATION_REQUEST', sOperationLoad, sStatusRunning, false /*withcommit*/ ,
					cb1
				);
			},
			(opId, cb1) => {
				operationId = opId;
				writeMessage.doIt(
					oTrace, oClient2Connection, sSchema, sNameDSO, oMetadata,
					operationId, [oMsgStart], true /*withCommit*/ , cb1
				);
			},
			cb1 => getNewId.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata, 'LOAD_REQUEST',
				null, null, false /*withcommit*/ , cb1
			),
			(lId, cb1) => {
				loadId = lId;
				return oClient.commit(cb1);
			},
			cb1 => writeAffectedRequests.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				operationId, [loadId], false /*withcommit*/ , cb1),
			cb1 => updateOperationStatus.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				operationId, sStatusFinished, false /*withCommit*/ , cb1
			),
			cb1 => {
				// create sql-statement with appropriate number of variables
				var sSql =
					'insert into ' + sActivationQueueTabName +
					'  select ' + loadId + ' as ' + loadIdColName + ',' +
					'    ROW_NUMBER() over () as ' + recordNoColName + ', * ' +
					'    from ( ' + sSQL + ' )';
				return oClient.prepare(sSql, cb1);
			},
			(statement, cb1) => statement.exec([], cb1),
			(rowCount, cb1) => {
				oMsgSuccess.text = _.replace(_.replace(oMsgSuccess.text, '&1', loadId), '&2', rowCount);
				return writeMessage.doIt(
					oTrace, oClient, sSchema, sNameDSO, oMetadata,
					operationId, [oMsgSuccess], false /*withCommit*/ , cb1
				);
			},
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
						operationId, aMessages, true /*withCommit*/ , cb1
					);
				},
				cb1 => oClient.rollback(cb1),
				cb1 => updateOperationStatus.doIt(
					oTrace, oClient2Connection, sSchema, sNameDSO, oMetadata,
					operationId, sStatusFailed, true /*withCommit*/ , cb1
				)
			],
			err1 => {
				if (err1) {
					oTrace.error('error in rollback of "storeSQL"');
					oTrace.error(err1);
					return cb(err1);
				}
				// do not send error to caller
				return cb(
					null, {
						'operationId': operationId,
						'loadId': loadId
					}
				);
			}
		) : cb(
			null, {
				'operationId': operationId,
				'loadId': loadId
			}
		)
	);
};