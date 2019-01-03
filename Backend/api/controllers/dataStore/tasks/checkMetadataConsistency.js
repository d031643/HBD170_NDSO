/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var getMetadata = require('./getMetadata');
var checkChecksum = require('./checkChecksum');
var getNewId = require('./getNewId');
var updateOperationStatus = require('./updateOperationStatus');
var writeMessage = require('./writeMessage');
var _ = require('lodash');

exports.doIt = function(oTrace, oClient, oClient2Connection, sSchema, sNameDSO, cb) {
	var operationId = null;
	var aCheckResults = [];
	var oMetadata = {};

	oTrace.info('call of "checkMetadataConsistency( ' + sSchema + ', ' + sNameDSO + ' )"');

	if (!sSchema) {
		return cb(new Error('No Schema provided'));
	}
	if (!sNameDSO) {
		return cb(new Error('No DataStore provided'));
	}

	// use constants from annotations
	var sOperation = 'CHECK_CONSISTENCY';
	var sStatusRunning = 'RUNNING';
	var sStatusFinished = 'FINISHED';
	var sStatusFailed = 'FAILED';

	var oMsgStart = {
		'type': 'I',
		'number': 7001,
		'text': 'Start check of metadata consistency'
	};
	var oMsgSuccess = {
		'type': 'S',
		'number': 7002,
		'text': 'Check of metadata consistency finished successfully.'
	};
	var oMsgFailed = {
		'type': 'E',
		'number': 7003,
		'text': 'Check of metadata consistency failed'
	};

	function MsgErrorDetail(sText) {
		var oMe = this;
		oMe.type = 'E';
		oMe.number = 99999;
		oMe.text = sText;
	}

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
					operationId, [oMsgStart], true /*withCommit*/ , cb1);
			},
			cb1 => async.waterfall(
				[
					cb2 => checkChecksum.doIt(
						oTrace, oClient, sSchema, sNameDSO, oMetadata, oMetadata.computeChecksum /*at*/ ,
						(oMetadata.computeChecksum && oMetadata.changeLog ? true : false) /*cl*/ , cb2
					),
					(oStored, oComputed, cb2) => {
						if (!oMetadata.computeChecksum && oStored.checksumCL) {
							return cb2(
								new Error('Nonempty checksum: "' + oStored.checksumCL + '" of Changelog stored')
							);
						}
						if (!oMetadata.computeChecksum && oStored.checksumAT) {
							return cb2(
								new Error('Nonempty checksum: "' + oStored.checksumAT + '" of active data stored')
							);
						}
						return cb2(null);
					}
				],
				err => {
					if (err) {
						aCheckResults.push({
							'msgType': 'E',
							'msgNumber': 7102,
							'msgText': err.message
						});
					}
					return cb1(null);
				}
			),
			cb1 => updateOperationStatus.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				operationId, sStatusFinished, false /*withCommit*/ , cb1
			),
			cb1 => {
				if (aCheckResults.length === 0) {
					aCheckResults.push({
						'msgType': 'I',
						'msgNumber': 7100,
						'msgText': 'CHECK-Result: No inconsistency found'
					});
				}
				aCheckResults.push(oMsgSuccess);
				return cb1(null);
			},
			cb1 => writeMessage.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				operationId, aCheckResults, false /*withCommit*/ , cb1
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