/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var hdbext = require('@sap/hdbext');
var _ = require('lodash');
var getMetadata = require('./getMetadata');
var getNewId = require('./getNewId');
var getRequestsForDeletion = require('./getRequestsForDeletion');
var updateOperationStatus = require('./updateOperationStatus');
var writeMessage = require('./writeMessage');
var writeAffectedRequests = require('./writeAffectedRequests');

exports.doIt = function(oTrace, oClient, oClient2Connection, sSchema, sNameDSO, aRequestIds, cb) {

	oTrace.info('call of "deleteRequest( [' + (aRequestIds ? aRequestIds.join(',') : '') + '], ' + sSchema + ', ' + sNameDSO + ' )"');

	if (!sSchema) {
		return cb(new Error('No Schema provided'));
	}
	if (!sNameDSO) {
		return cb(new Error('No DataStore provided'));
	}
	if (!aRequestIds || aRequestIds.length === 0) {
		return cb(new Error('No List of requests provided'));
	}

	var aReqIds = _.map(aRequestIds, x => parseInt(x));

	// use constants from annotations
	var sOperationDelete = 'DELETE_REQUEST';
	var sStatusRunning = 'RUNNING';
	var sStatusFinished = 'FINISHED';
	var sStatusFailed = 'FAILED';

	var oMsgStart = {
		'type': 'I',
		'number': 3001,
		'text': 'Start deletion of requests [&1]'
	};
	var oMsgSuccess = {
		'type': 'S',
		'number': 3002,
		'text': 'Deletion finished successfully. #Lines=&1'
	};
	var oMsgFailed = {
		'type': 'E',
		'number': 3003,
		'text': 'Deletion failed'
	};

	function MsgErrorDetail(sText) {
		var oMe = this;
		oMe.type = 'E';
		oMe.number = 99999;
		oMe.text = sText;
	}

	var loadIdColName = '"' + 'technicalKey.loadId' + '"';

	var oMetadata = {};
	var operationId = null;

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
				oMsgStart.text = _.replace(oMsgStart.text, '&1', aReqIds.join(','));
				return writeMessage.doIt(
					oTrace, oClient2Connection, sSchema, sNameDSO, oMetadata,
					operationId, [oMsgStart], true /*withCommit*/ , cb1
				);
			},
			cb1 => oClient.commit(cb1),
			cb1 => getRequestsForDeletion.doIt(
				oTrace, oClient, sSchema, sNameDSO, cb1
			),
			(oResult, cb1) => {
				var a = _.map(oResult.result, line => line.requestId);
				var aDiff = _.difference(aReqIds, a);
				return (aDiff.length !== 0) ? cb1(
					new Error(
						'Non-eligable requests must not be selected:' +
						aDiff.join(',')
					)
				) : cb1(null);
			},
			cb1 => writeAffectedRequests.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				operationId, aReqIds, false /*withCommit*/ , cb1
			),
			cb1 => updateOperationStatus.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				operationId, sStatusFinished, false /*withCommit*/ , cb1
			),
			cb1 => async.mapSeries(
				oMetadata.activationQueues,
				(activationQueue, cb2) => async.waterfall(
					[
						cb3 => {
							var sActivationQueueTabName = [
								'"',
								hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
								'"."',
								hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
								'.',
								hdbext.sqlInjectionUtils.escapeDoubleQuotes(activationQueue.name),
								'"'
							].join('');
							var sSql =
								'delete from ' + sActivationQueueTabName +
								'  where ' + loadIdColName + ' = ?';
							return oClient.prepare(sSql, cb3);
						},
						(statement, cb3) => statement.exec(
							aReqIds.map(n => [n]), cb3
						),
						(affectedRows, cb3) => cb3(null, {
							'activationQueue': activationQueue.name,
							'rowCount': affectedRows
						})
					],
					(err1, o) => cb2(err1, o)
				),
				(err, aResults) => cb1(err, aResults)
			),
			(aResults, cb1) => {
				oMsgSuccess.text = _.replace(
					oMsgSuccess.text, '&1',
					_.reduce(
						aResults,
						(sum, e) => e.rowCount + sum,
						0
					)
				);
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
			err1 => err1 ? cb(err1) : cb(null, {
				'operationId': operationId
			})
		) : cb(null, {
			'operationId': operationId
		})
	);
};