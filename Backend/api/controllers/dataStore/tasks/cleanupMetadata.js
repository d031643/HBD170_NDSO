/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var _ = require('lodash');
var getMetadata = require('./getMetadata');
var getNewId = require('./getNewId');
var updateOperationStatus = require('./updateOperationStatus');
var writeMessage = require('./writeMessage');
var setOperationDetails = require('./setOperationDetails');

exports.doIt = function(oTrace, oClient, oClient2Connection, sSchema, sNameDSO, maxRequestId, maxTimestamp, cb) {
	var operationId = null;
	oTrace.info('call of "cleanupMetadata( ' + maxRequestId + ', ' + sSchema + ', ' + sNameDSO + ' )"');

	if (!sSchema) {
		return cb(new Error('No Schema provided'));
	}
	if (!sNameDSO) {
		return cb(new Error('No DataStore provided'));
	}

	// use constants from annotations
	var sOperation = 'CLEANUP_METADATA';
	var sStatusRunning = 'RUNNING';
	var sStatusFinished = 'FINISHED';
	var sStatusFailed = 'FAILED';

	var oMsgStart = {
		'type': 'I',
		'number': 6001,
		'text': 'Start Cleanup of Metadata of requests &1'
	};
	var oMsgSuccess = {
		'type': 'S',
		'number': 6002,
		'text': 'Cleanup of Metadata finished successfully. #Lines=&1'
	};
	var oMsgFailed = {
		'type': 'E',
		'number': 6003,
		'text': 'Cleanup of Metadata failed'
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
					operationId, [oMsgStart], true /*withCommit*/ , cb1
				);
			},
			cb1 => oClient.commit(cb1),
			cb1 => setOperationDetails.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				operationId, {
					'maxRequestId': maxRequestId,
					'maxTimestamp': maxTimestamp
				},
				false /*withCommit*/ , cb1
			),
			cb1 => updateOperationStatus.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				operationId, sStatusFinished, false /*withCommit*/ , cb1
			),
			cb1 => {
				oMsgSuccess.text = _.replace(oMsgSuccess.text, '&1', 0);
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