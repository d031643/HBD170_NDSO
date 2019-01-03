/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var _ = require('lodash');
var util = require('util');
var hdbext = require('@sap/hdbext');
var checkChecksum = require('./checkChecksum');
var getMetadata = require('./getMetadata');
var getNewId = require('./getNewId');
var getRequestsForActivation = require('./getRequestsForActivation');
var updateChecksum = require('./updateChecksum');
var updateOperationStatus = require('./updateOperationStatus');
var writeMessage = require('./writeMessage');
var writeAffectedRequests = require('./writeAffectedRequests');
var writeAggregationHistory = require('./writeAggregationHistory');

exports.doIt = function(oTrace, oClient, oClient2Connection, sSchema, sNameDSO,
	aLoadRequestIds, cb) {
	var sSql = 'CALL DSO_ACTIVATE_CHANGES(?, ?, ?, ?, ?, ?, ?)';
	oTrace.info('call of "activate( [' + (aLoadRequestIds ? aLoadRequestIds.join(',') : '') + '], ' + sSchema + ', ' + sNameDSO + ' )"');

	if (!sSchema) {
		return cb(new Error('No Schema provided'));
	}
	if (!sNameDSO) {
		return cb(new Error('No DataStore provided'));
	}
	if (!aLoadRequestIds || aLoadRequestIds.length === 0) {
		return cb(new Error('No List of load requests provided'));
	}

	var aLoadReqIds = _.map(aLoadRequestIds, x => parseInt(x));

	var operationColName = '"' + 'technicalAttributes.operation' + '"';
	var statusColName = '"' + 'technicalAttributes.status' + '"';

	var loadIdName = 'technicalKey.loadId';
	var loadIdColName = '"' + 'technicalKey.loadId' + '"';

	var sOperationActivate = 'ACTIVATE';
	var sStatusRunning = 'RUNNING';
	var sStatusFinished = 'FINISHED';
	var sStatusFailed = 'FAILED';

	var oMsgStart = {
		'type': 'I',
		'number': 2001,
		'text': 'Start activation of requests [&1]'
	};
	var oMsgSuccess = {
		'type': 'S',
		'number': 2002,
		'text': 'Activation finished successfully. ActivationId = &1'
	};
	var oMsgFailed = {
		'type': 'E',
		'number': 2003,
		'text': 'Activation failed'
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
	var sOperationHistoryTabName = null;
	var operationId = null;
	var activationId = null;
	var callProc = null;
	return async.waterfall(
		[
			cb1 => oClient.prepare(sSql, cb1),
			(statement, cb1) => {
				callProc = statement;
				return cb1();
			},
			cb1 => getMetadata.doIt(oTrace, oClient, sSchema, sNameDSO, cb1),
			(oMD, cb1) => {
				sOperationHistoryTabName = [
					'"',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
					'"."',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO), '.',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(oMD.operationHistory.name),
					'"'
				].join('');
				oMetadata = oMD;
				return cb1(
					null,
					(
						sOperationHistoryTabName,
						oMetadata
					)
				);
			},
			(oMetdata, cb1) => getNewId.doIt(
				oTrace,
				oClient,
				sSchema,
				sNameDSO,
				oMetadata,
				'OPERATION_REQUEST',
				sOperationActivate,
				sStatusRunning,
				false /*withCommit*/ ,
				cb1
			),
			(opId, cb1) => {
				operationId = opId;
				oMsgStart.text = _.replace(oMsgStart.text, '&1', aLoadReqIds.join(','));
				return writeMessage.doIt(
					oTrace, oClient2Connection, sSchema, sNameDSO, oMetadata,
					opId, [oMsgStart], true /*withCommit*/ , cb1
				);
			},
			cb1 => getNewId.doIt(
				oTrace,
				oClient,
				sSchema,
				sNameDSO,
				oMetadata,
				'ACTIVATION_REQUEST', null, null, false /*withCommit*/ ,
				cb1
			),
			(actId, cb1) => {
				activationId = actId;
				return oClient.commit(cb1);
			},
			cb1 => getRequestsForActivation.doIt(
				oTrace, oClient, sSchema, sNameDSO,
				_.reduce(
					aLoadReqIds,
					(max, id) => id > max ? id : max,
					0), cb1),
			(oR, cb1) => {
				var a = oR.result.map(o => o['requestId']);
				a.sort();
				var a1 = [].concat(aLoadReqIds.sort());
				a1.sort();
				return (
					a.length !== a1.length ||
					a1.some((reqId, index) => reqId !== a[index])
				) ? cb1(
					new Error(
						[
							'Invalid list of loadids:',
							a.join(),
							'\n',
							'Required: ',
							a1.join()
						].join('')
					)
				) : cb1();
			},
			cb1 => writeAffectedRequests.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				operationId, _.flatten([aLoadReqIds, activationId]), false /*withCommit*/ ,
				cb1
			),
			cb1 => oClient.prepare(
				[
					'update ',
					sOperationHistoryTabName, // already properly escaped above
					'  set ' + statusColName,
					' = \'DELETED\'', // already properly escaped above
					'  where ',
					operationColName,
					' = \'ACTIVATE\'', // already properly escaped above
					'    and ',
					statusColName, // already properly escaped above
					' = \'FAILED\''
				].join(''), cb1
			),
			(statement, cb1) => statement.exec([], cb1),
			(a, cb1) => !oMetadata.computeChecksum ? cb1(null) : checkChecksum.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				true /*forAT*/ , (oMetadata.changeLog ? true : false) /*forCL*/ ,
				cb1
			),
			(o1, o2, cb1) => updateOperationStatus.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				operationId, sStatusFinished, false /*withCommit*/ ,
				cb1
			),
			cb1 => async.mapSeries(
				oMetadata.activationQueues,
				(oActivationQueue, cb2) => async.waterfall(
					[
						cb3 => oClient.prepare(sSql, cb3),
						(statement, cb3) => {
							var oParameter = null;
							try {
								oParameter = {
									'odsName': sNameDSO,
									'activationId': activationId,
									'updateOps': oActivationQueue.fields.map(
										field => {
											var aFieldAT = _.filter(
												oMetadata.activeData.fields,
												field1 => field1.name === field.name
											);
											var aggregationAT = (
												aFieldAT.length === 0 ? 'NOP' : aFieldAT[0].aggregation
											);
											if (field.aggregation === 'NOP' && aggregationAT === 'NOP') {
												return;
											}
											if (typeof(field.aggregation) !== 'string') {
												throw new Error(
													[
														'Aggregation of field: ',
														field.name,
														' invalid: ',
														field.aggregation
													].join('')
												);
											}
											return {
												column: field.name,
												aggregationBehavior: field.aggregation,
												negateBeforeImages: aggregationAT === 'SUM' ? true : false
											};
										}
									).filter(_.identity),
									'sourceTableInfo': {
										'filter': {
											'column': loadIdName,
											'values': aLoadReqIds
										}
									},
									'completeSnapshotDeleteOthers': (oMetadata.snapshotSupport ? true : false)
								};
							} catch (err) {
								return cb3(err);
							}
							oTrace.log('Parameters for activation: ');
							oTrace.log(JSON.stringify(oParameter));
							if (oMetadata.changeLog) {
								oParameter['changeLogInfo'] = {
									'changeLogSchemaName': sSchema,
									'changeLogTableName': sNameDSO + '.' + oMetadata.changeLog.name
								};
							}
							return callProc.exec({
								'SOURCE_SCHEMA_NAME': sSchema,
								'SOURCE_TABLE_NAME': sNameDSO + '.' + oActivationQueue.name,
								'TARGET_SCHEMA_NAME': sSchema,
								'TARGET_TABLE_NAME': sNameDSO + '.' + oMetadata.activeData.name,
								'USAGE_MODE': 'CDSO',
								'PARAMETER': JSON.stringify(oParameter)
							}, cb3);
						}
					],
					cb2
				),
				(err, aResults) => cb1(err, aResults)
			),
			// delete activated records out of the inbound-queues
			(aResults, cb1) => async.mapSeries(
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
							return oClient.prepare(
								[
									'delete from ',
									sActivationQueueTabName,
									'  where ',
									loadIdColName,
									' = ?'
								].join(''),
								cb3
							);
						},
						(statement, cb3) => statement.exec(aLoadReqIds.map(n => [n]), cb3)
					],
					cb2
				),
				cb1
			),
			(results, cb1) => writeAggregationHistory.doIt(
				oTrace, oClient, sSchema, sNameDSO,
				oMetadata, activationId, false /*withCommit*/ , cb1
			),
			cb1 => (!oMetadata.computeChecksum) ? cb1(null, {}) : updateChecksum.doIt(
				oTrace, oClient, sSchema, sNameDSO,
				oMetadata, operationId, true /*updateAT*/ , (oMetadata.changeLog ? true : false) /*updateCL*/ ,
				cb1
			),
			(aResults, cb1) => {
				oMsgSuccess.text = _.replace(oMsgSuccess.text, '&1', activationId);
				var oMsgSuccessDetail = new MsgSuccessDetail(JSON.stringify(aResults));
				return writeMessage.doIt(
					oTrace, oClient, sSchema, sNameDSO, oMetadata,
					operationId, [oMsgSuccessDetail, oMsgSuccess], false /*withCommit*/ ,
					cb1);
			},
			cb1 => oClient.commit(cb1)
		],
		err => err ? async.waterfall(
			[
				cb1 => {
					var aMessages = [];
					if (err.aNdsoErrorDetails && err.aNdsoErrorDetails.length !== 0) {
						aMessages = _.flatten(_.map(err.aNdsoErrorDetails, function(oDetail) {
							return _.map(oDetail,
								oV => new MsgErrorDetail(util.inspect(oV, null, false)));
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
					oTrace, oClient, sSchema, sNameDSO, oMetadata,
					operationId, sStatusFailed, true /*withCommit*/ , cb1
				)
			],
			err1 => {
				oTrace.log('SQL: ');
				oTrace.log(sSql);
				if (err1) {
					oTrace.error('error in rollback of "activate"');
					oTrace.error(err1);
					return cb(err1);
				}
				if (!operationId) {
					return cb(err);
				}
				// do not send error to caller

				return cb(null, {
					'operationId': operationId,
					'activationId': activationId
				});
			}) : cb(null, {
			'operationId': operationId,
			'activationId': activationId
		})
	);
};