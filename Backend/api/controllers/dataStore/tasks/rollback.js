/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var _ = require('lodash');
var hdbext = require('@sap/hdbext');
var util = require('util');
var checkChecksum = require('./checkChecksum');
var getMetadata = require('./getMetadata');
var getNewId = require('./getNewId');
var getRequestsForRollback = require('./getRequestsForRollback');
var updateChecksum = require('./updateChecksum');
var updateOperationStatus = require('./updateOperationStatus');
var writeMessage = require('./writeMessage');
var writeAffectedRequests = require('./writeAffectedRequests');
var rollbackSeries = require('./rollbackSeries');

exports.doIt = function(oTrace, oClient, oClient2Connection, sSchema, sNameDSO, aActivationIds, cb) {
	var operationId = null;
	var minRequest = null;
	var aResults = null;
	oTrace.info('call of "rollback( [' + (aActivationIds ? aActivationIds.join(',') : '') + '], ' + sSchema + ', ' + sNameDSO + ' )"');

	if (!sSchema) {
		cb(new Error('No Schema provided'));
		return;
	}
	if (!sNameDSO) {
		cb(new Error('No DataStore provided'));
		return;
	}
	if (!aActivationIds || aActivationIds.length === 0) {
		cb(new Error('No List of activation requests provided'));
		return;
	}

	var aActIds = _.map(aActivationIds, x => parseInt(x));

	// use constants from annotations
	var requestIdColName = '"' + 'technicalKey.requestId' + '"';
	var operationIdColName = '"' + 'technicalKey.operationId' + '"';
	var idColName = '"' + 'technicalKey.id' + '"';
	var typeColName = '"' + 'technicalAttributes.type' + '"';

	var sAggregationHistoryTabName = '';
	var sIdGeneratorTabName = '';
	var sAffectedRequestsTabName = '';

	var sOperationRollback = 'ROLLBACK';
	var sStatusRunning = 'RUNNING';
	var sStatusFinished = 'FINISHED';
	var sStatusFailed = 'FAILED';

	var oMsgStart = {
		'type': 'I',
		'number': 4001,
		'text': 'Start rollback of requests [&1]'
	};
	var oMsgSuccess = {
		'type': 'S',
		'number': 4002,
		'text': 'Rollback finished successfully.'
	};
	var oMsgFailed = {
		'type': 'E',
		'number': 4003,
		'text': 'Rollback failed'
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
				sAggregationHistoryTabName = [
					'"',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
					'"."',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
					'.',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(oMetadata.aggregationHistory.name),
					'"'
				].join('');
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
				'OPERATION_REQUEST', sOperationRollback, sStatusRunning, false /*withCommit*/ ,
				cb1),
			(n, cb1) => {
				operationId = n;
				oMsgStart.text = _.replace(oMsgStart.text, '&1', aActIds.join(','));
				return writeMessage.doIt(
					oTrace, oClient2Connection, sSchema, sNameDSO, oMetadata,
					operationId, [oMsgStart], true /*withCommit*/ ,
					cb1
				);
			},
			cb1 => oClient.commit(cb1),
			cb1 => {
				minRequest = _.reduce(
					aActIds,
					function(min, id) {
						return id < min ? id : min;
					}, Infinity);
				return getRequestsForRollback.doIt(
					oTrace, oClient, sSchema, sNameDSO, minRequest, cb1
				);
			},
			(oResult, cb1) => {
				var a = _.map(oResult.result, line => line.requestId);
				var aDiff1 = _.difference(a, aActIds);
				var aDiff2 = _.difference(aActIds, a);
				if (aDiff1.length !== 0) {
					cb1(
						new Error(
							'All eligable requests >= ' + minRequest + ' must be selected:' +
							aDiff1.join(',')
						)
					);
					return;
				} else if (aDiff2.length !== 0) {
					return cb1(
						new Error(
							'Non-eligable requests must not be selected:' +
							aDiff2.join(','))
					);
				}
				return cb1(null);
			},
			cb1 => !oMetadata.changeLog ? cb1(
				new Error('Rollback not possible as no Changelog exists')
			) : (function() {
				// get all load-requests which are 'affected' by the given activation-requests
				var sSql =
					'select ID_GEN.' + idColName + ' as "id" ' +
					'  from ' + sIdGeneratorTabName + ' ID_GEN ' +
					'  join ' + sAffectedRequestsTabName + ' AFF_REQ1 ' +
					'    on ID_GEN.' + idColName + ' = AFF_REQ1.' + requestIdColName +
					'  join ' + sAffectedRequestsTabName + ' AFF_REQ2 ' +
					'    on AFF_REQ1.' + operationIdColName + ' = AFF_REQ2.' + operationIdColName +
					'  where ID_GEN.' + typeColName + ' = \'LOAD_REQUEST\'' +
					'    and AFF_REQ2.' + requestIdColName + ' in ( ' + aActIds.join(',') + ' )';
				return oClient.prepare(sSql, cb1);
			}()),
			(statement, cb1) => statement.execute(null, cb1),
			(aRows, cb1) => cb1(null, _.map(aRows, row => row.id)),
			(aLoadIds, cb1) => writeAffectedRequests.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				operationId, _.flatten([aActIds, aLoadIds]), false /*withCommit*/ , cb1
			),
			cb1 => (!oMetadata.computeChecksum) ? cb1(null) : checkChecksum.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				true /*forAT*/ , true /*forCL*/ , cb1
			),
			cb1 => updateOperationStatus.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				operationId, sStatusFinished, false /*withCommit*/ , cb1
			),
			cb1 => {
				var sSql =
					'select * ' +
					'  from ' + sAggregationHistoryTabName +
					'  where ' + requestIdColName + ' in (' + aActivationIds.join(',') + ')';
				return oClient.prepare(sSql, cb1);
			},
			(statement, cb1) => statement.execute(null, cb1),
			(aAggregationHistory, cb1) => {
				return cb1(null, aAggregationHistory);
			},

			(aAggregationHistory, cb1) => rollbackSeries.doIt(
				oTrace, oClient, sSchema, sNameDSO,
				oMetadata, aActIds, aAggregationHistory, cb1
			),
			(aR, cb1) => {
				aResults = aR;
				if (!oMetadata.computeChecksum) {
					cb1(null, aResults);
					return;
				}
				return updateChecksum.doIt(
					oTrace, oClient, sSchema, sNameDSO,
					oMetadata, operationId, true /*updateAT*/ , true /*updateCL*/ , cb1
				);
			},
			cb1 => writeMessage.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				operationId, [new MsgSuccessDetail(JSON.stringify(aResults)), oMsgSuccess], false /*withCommit*/ ,
				cb1
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
									oV => new MsgErrorDetail(util.inspect(oV, null, false))
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
					oTrace.error('error in rollback of "rollback"');
					oTrace.error(err1);
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