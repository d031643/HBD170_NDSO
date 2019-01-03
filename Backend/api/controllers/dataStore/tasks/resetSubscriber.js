/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var hdbext = require('@sap/hdbext');
var _ = require('lodash');
var getMetadata = require('./getMetadata');
var getNewId = require('./getNewId');
var updateOperationStatus = require('./updateOperationStatus');
var setOperationDetails = require('./setOperationDetails');
var writeMessage = require('./writeMessage');

exports.doIt = function(oTrace, oClient, oClient2Connection, sSchema, sNameDSO,
	subscriberName, cb) {

	var operationId = null;

	oTrace.info('call of "resetSubscriber( ' + subscriberName + ', ' + sSchema + ', ' + sNameDSO + ' )"');

	if (!sSchema) {
		cb(new Error('No Schema provided'));
		return;
	}
	if (!sNameDSO) {
		cb(new Error('No DataStore provided'));
		return;
	}
	if (!subscriberName) {
		cb(new Error('No subscriber-name provided'));
		return;
	}

	// use constants from annotations
	var sOperation = 'RESET_SUBSCRIBER';
	var sStatusRunning = 'RUNNING';
	var sStatusFinished = 'FINISHED';
	var sStatusFailed = 'FAILED';

	var oMsgStart = {
		'type': 'I',
		'number': 12001,
		'text': 'Start reset-subscriber'
	};
	var oMsgSuccess = {
		'type': 'S',
		'number': 12002,
		'text': 'Reset-subscriber finished successfully.'
	};
	var oMsgFailed = {
		'type': 'E',
		'number': 12003,
		'text': 'Reset-subscriber failed'
	};

	function MsgErrorDetail(sText) {
		var oMe = this;
		oMe.type = 'E';
		oMe.number = 99999;
		oMe.text = sText;
	}

	var subscriberNameColName = '"' + 'technicalKey.subscriberName' + '"';
	var maxRequestColName = '"' + 'technicalAttributes.maxRequest' + '"';

	var oMetadata = {};

	return async.waterfall(
		[
			cb1 => getMetadata.doIt(
				oTrace, oClient, sSchema, sNameDSO, cb1),
			(oMD, cb1) => {
				oMetadata = oMD;
				return cb1();
			},
			cb1 => getNewId.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				'OPERATION_REQUEST', sOperation, sStatusRunning, false /*withCommit*/ , cb1
			),
			(opId, cb1) => {
				operationId = opId;
				return writeMessage.doIt(
					oTrace, oClient2Connection, sSchema, sNameDSO, oMetadata,
					operationId, [oMsgStart], true /*withCommit*/ , cb1);
			},
			cb1 => oClient.commit(cb1),
			cb1 => setOperationDetails.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				operationId, {
					'subscriberName': subscriberName
				},
				false /*withCommit*/ , cb1),

			cb1 => updateOperationStatus.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				operationId, sStatusFinished, false /*withCommit*/ , cb1
			),
			cb1 => {
				var sSubscribersTabname = [
					'"',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
					'"."',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
					'.',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(oMetadata.subscribers.name),
					'"'
				].join('');

				var sSql =
					'update ' + sSubscribersTabname +
					'  set ' + maxRequestColName + ' = 0' +
					'  where ' + subscriberNameColName + ' = ?';
				return oClient.prepare(sSql, cb1);
			},
			(statement, cb1) => statement.exec([subscriberName], cb1),
			(o, cb1) => cb1(),
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
						aMessages = _.flatten(
							_.map(
								err.aNdsoErrorDetails,
								oDetail => _.map(
									oDetail,
									oV => new MsgErrorDetail(oV.toString())
								)
							)
						);
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
					oTrace.error('error in rollback of "resetSubscriber"');
					oTrace.error(err1);
					return cb(err1);
				}
				// do not send error to caller
				return cb(null, {
					'operationId': operationId
				});
			}
		) : cb(null, {
			'operationId': operationId
		})
	);
};