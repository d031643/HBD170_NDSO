/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var hdbext = require('@sap/hdbext');
var _ = require('lodash');
var checkChecksum = require('./checkChecksum');
var getMetadata = require('./getMetadata');
var getNewId = require('./getNewId');
var getRequestsForCleanup = require('./getRequestsForCleanup');
var updateChecksum = require('./updateChecksum');
var updateOperationStatus = require('./updateOperationStatus');
var setOperationDetails = require('./setOperationDetails');
var writeMessage = require('./writeMessage');
var writeAffectedRequests = require('./writeAffectedRequests');

exports.doIt = function(oTrace, oClient, oClient2Connection, sSchema, sNameDSO, aRequestIds, cb) {
	var operationId = null;
	var aLoadIds = null;
	var rowCount = null;
	oTrace.info('call of "cleanupChangelog( [' + (aRequestIds ? aRequestIds.join(',') : '') + '], ' + sSchema + ', ' + sNameDSO + ' )"');

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
	var sOperation = 'CLEANUP_CHANGELOG';
	var sStatusRunning = 'RUNNING';
	var sStatusFinished = 'FINISHED';
	var sStatusFailed = 'FAILED';

	var oMsgStart = {
		'type': 'I',
		'number': 8001,
		'text': 'Start of Cleanup of Changelog of request &1'
	};
	var oMsgSuccess = {
		'type': 'S',
		'number': 8002,
		'text': 'Cleanup of Changelog finished successfully. #Lines=&1'
	};
	var oMsgFailed = {
		'type': 'E',
		'number': 8003,
		'text': 'Cleanup of Changelog failed'
	};

	function MsgErrorDetail(sText) {
		var oMe = this;
		oMe.type = 'E';
		oMe.number = 99999;
		oMe.text = sText;
	}

	var requestIdColName = '"' + 'technicalKey.requestId' + '"';
	var operationIdColName = '"' + 'technicalKey.operationId' + '"';
	var activationIdColName = '"' + 'technicalKey.activationId' + '"';
	var idColName = '"' + 'technicalKey.id' + '"';
	var typeColName = '"' + 'technicalAttributes.type' + '"';

	var sChangelogTabName = '';
	var sIdGeneratorTabName = '';
	var sAffectedRequestsTabName = '';

	var oMetadata = {};

	return async.waterfall(
		[
			cb1 => getMetadata.doIt(
				oTrace, oClient, sSchema, sNameDSO, cb1
			),
			(oMD, cb1) => {
				oMetadata = oMD;
				if (oMetadata.changeLog) {
					sChangelogTabName = [
						'"',
						hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
						'"."',
						hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
						'.',
						hdbext.sqlInjectionUtils.escapeDoubleQuotes(oMetadata.changeLog.name),
						'"'
					].join('');
				}
				sAffectedRequestsTabName = [
					'"',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
					'"."',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
					'.',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(oMetadata.affectedRequests.name),
					'"'
				].join('');
				sIdGeneratorTabName = [
					'"',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
					'"."',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
					'.',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(oMetadata.idGenerator.name),
					'"'
				].join('');
				return cb1();
			},
			cb1 => getNewId.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				'OPERATION_REQUEST', sOperation, sStatusRunning, false /*withCommit*/ , cb1
			),
			(opId, cb1) => {
				operationId = opId;
				oMsgStart.text = _.replace(oMsgStart.text, '&1', aRequestIds.join(','));
				return writeMessage.doIt(
					oTrace, oClient2Connection, sSchema, sNameDSO, oMetadata,
					operationId, [oMsgStart], true /*withCommit*/ , cb1
				);
			},
			cb1 => oClient.commit(cb1),
			cb1 => getRequestsForCleanup.doIt(
				oTrace, oClient, sSchema, sNameDSO,
				_.reduce(
					aReqIds,
					(max, id) => id > max ? id : max,
					0
				),
				null, cb1
			),
			(oResult, cb1) => {
				var a = _.map(oResult.result, line => line.requestId);
				var aDiff1 = _.difference(a, aReqIds);
				var aDiff2 = _.difference(aReqIds, a);
				return (aDiff1.length !== 0) ? cb1(
					new Error(
						'All eligable requests must be selected:' +
						aDiff1.join(',')
					)
				) : (aDiff2.length !== 0) ? cb1(
					new Error(
						'Non-eligable requests must not be selected:' +
						aDiff2.join(',')), operationId) : cb1(null);
			},
			cb1 => !oMetadata.changeLog ? cb1(
				new Error('Cleanup not possible as no Changelog exists')
			) : setOperationDetails.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				operationId, {
					'requestFilter': aRequestIds
				}, false /*withCommit*/ , cb1
			),
			// get all load-requests which are 'affected' by the given activation-requests
			cb1 => oClient.prepare(
				'select ID_GEN.' + idColName + ' as "id" ' +
				'  from ' + sIdGeneratorTabName + ' ID_GEN ' +
				'  join ' + sAffectedRequestsTabName + ' AFF_REQ1 ' +
				'    on ID_GEN.' + idColName + ' = AFF_REQ1.' + requestIdColName +
				'  join ' + sAffectedRequestsTabName + ' AFF_REQ2 ' +
				'    on AFF_REQ1.' + operationIdColName + ' = AFF_REQ2.' + operationIdColName +
				'  where ID_GEN.' + typeColName + ' = \'LOAD_REQUEST\'' +
				'    and AFF_REQ2.' + requestIdColName + ' in ( ' + aReqIds.join(',') + ' )',
				cb1
			),
			(statement, cb1) => statement.exec([], cb1),
			(aRows, cb1) => {
				aLoadIds = _.map(aRows, row => row.id);
				return cb1(null);
			},
			cb1 => writeAffectedRequests.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				operationId, _.flatten([aLoadIds, aReqIds]), false /*withCommit*/ , cb1
			),
			cb1 => (!oMetadata.computeChecksum) ? cb1(null, {}, {}) : checkChecksum.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				false /*forAT*/ , true /*forCL*/ , cb1
			),
			(o1, o2, cb1) => updateOperationStatus.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				operationId, sStatusFinished, false /*withCommit*/ , cb1
			),
			cb1 => oClient.prepare(
				'delete from ' + sChangelogTabName +
				'  where ' + activationIdColName + ' = ?', cb1
			),
			(statement, cb1) => statement.exec(
				aReqIds.map(n => [n]),
				cb1
			),
			(rC, cb1) => {
				rowCount = rC;
				return (!oMetadata.computeChecksum) ? cb1(null, null) : updateChecksum.doIt(
					oTrace, oClient, sSchema, sNameDSO,
					oMetadata, operationId, false /*updateAT*/ , true /*updateCL*/ , cb1
				);
			},
			(o, cb1) => {
				oMsgSuccess.text = _.replace(oMsgSuccess.text, '&1', rowCount);
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
					operationId, sStatusFailed, true /*withCommit*/ ,
					cb1
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