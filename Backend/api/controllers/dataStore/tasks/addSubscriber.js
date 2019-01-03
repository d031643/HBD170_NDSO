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
	subscriberName, description, cb) {

	var operationId = null;
	oTrace.info('call of "addSubscriber( ' + subscriberName + ', ' + description + ', ' + sSchema + ', ' + sNameDSO + ' )"');

	if (!sSchema) {
		return cb(new Error('No Schema provided'));
	}
	if (!sNameDSO) {
		return cb(new Error('No DataStore provided'));
	}
	if (!subscriberName) {
		return cb(new Error('No subscriber-name provided'));
	}

	// use constants from annotations
	var sOperation = 'ADD_SUBSCRIBER';
	var sStatusRunning = 'RUNNING';
	var sStatusFinished = 'FINISHED';
	var sStatusFailed = 'FAILED';

	var oMsgStart = {
		'type': 'I',
		'number': 10001,
		'text': 'Start add-subscriber: "&1"'
	};
	var oMsgSuccess = {
		'type': 'S',
		'number': 10002,
		'text': 'Add-subscriber finished successfully.'
	};
	var oMsgFailed = {
		'type': 'E',
		'number': 10003,
		'text': 'Add-subscriber failed'
	};

	function MsgErrorDetail(sText) {
		var oMe = this;
		oMe.type = 'E';
		oMe.number = 99999;
		oMe.text = sText;
	}

	var subscriberNameColName = '"' + 'technicalKey.subscriberName' + '"';
	var descriptionColName = '"' + 'technicalAttributes.description' + '"';
	var userNameColName = '"' + 'technicalAttributes.userName' + '"';
	var creationTimestampColName = '"' + 'technicalAttributes.creationTimestamp' + '"';
	var maxRequestColName = '"' + 'technicalAttributes.maxRequest' + '"';
	var pushNotificationColName = '"' + 'technicalAttributes.pushNotification' + '"';

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
				oMsgStart.text = _.replace(oMsgStart.text, '&1', subscriberName);
				return writeMessage.doIt(
					oTrace, oClient2Connection, sSchema, sNameDSO, oMetadata,
					operationId, [oMsgStart], true /*withCommit*/ , cb1
				);
			},
			cb1 => oClient.commit(cb1),
			cb1 => setOperationDetails.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				operationId, {
					'subscriberName': subscriberName
				}, false /*withCommit*/ , cb1
			),
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

				return oClient.prepare(
					'insert into ' + sSubscribersTabname + ' ( ' + subscriberNameColName + ', ' + descriptionColName + ', ' + userNameColName + ', ' +
					creationTimestampColName + ', ' + maxRequestColName + ', ' + pushNotificationColName + ' )' + ' values ( ?, ?, ' + 'CURRENT_USER, ' +
					'CURRENT_UTCTIMESTAMP, ' + '0,' + '\'\' )', cb1
				);
			},
			(statement, cb1) => statement.exec([subscriberName, description], cb1),
			(o, cb1) => writeMessage.doIt(
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
			err1 => (err1) ? cb(err1) : cb(null, {
				'operationId': operationId
			})
		) : cb(null, {
			'operationId': operationId
		})
	);
};