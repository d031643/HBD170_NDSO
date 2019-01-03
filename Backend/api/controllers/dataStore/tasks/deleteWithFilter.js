/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var _ = require('lodash');
var hdbext = require('@sap/hdbext');
var checkChecksum = require('./checkChecksum');
var getMetadata = require('./getMetadata');
var getNewId = require('./getNewId');
var setOperationDetails = require('./setOperationDetails');
var updateChecksum = require('./updateChecksum');
var updateOperationStatus = require('./updateOperationStatus');
var writeAffectedRequests = require('./writeAffectedRequests');
var writeMessage = require('./writeMessage');

exports.doIt = function(oTrace, oClient, oClient2Connection, sSchema, sNameDSO, sWhere, propagateDeletion, cb) {
	var operationId = null;
	var activationId = null;
	var rowCountCL = null;
	var sSql = null;
	var aResult = null;

	oTrace.info('call of "deleteWithFilter( ' + sWhere + ', ' + propagateDeletion + ', ' + sSchema + ', ' + sNameDSO + ' )"');

	if (!sSchema) {
		return cb(new Error('No Schema provided'));
	}
	if (!sNameDSO) {
		return cb(new Error('No DataStore provided'));
	}
	if (!sWhere) {
		return cb(new Error('No filter provided'));
	}

	// use constants from annotations
	var sOperationDelete = 'DELETE_WITH_FILTER';
	var sStatusRunning = 'RUNNING';
	var sStatusFinished = 'FINISHED';
	var sStatusFailed = 'FAILED';

	var oMsgStart = {
		'type': 'I',
		'number': 14001,
		'text': 'Start deletion with filter (Propagate-Deletion=&1): [&2]'
	};
	var oMsgSuccess = {
		'type': 'S',
		'number': 14002,
		'text': 'Deletion finished successfully. ChangeLogId=&1; #Lines=&2'
	};
	var oMsgFailed = {
		'type': 'E',
		'number': 14003,
		'text': 'Deletion failed'
	};

	function MsgErrorDetail(sText) {
		var oMe = this;
		oMe.type = 'E';
		oMe.number = 99999;
		oMe.text = sText;
	}

	var activationIdColName = '"' + 'technicalKey.activationId' + '"';
	var recordNoColName = '"' + 'technicalKey.recordNo' + '"';
	var recordModeColName = '"' + 'technicalAttributes.recordMode' + '"';

	var sActiveDataTabName = '';
	var sChangeLogTabName = '';

	var oMetadata = {};

	return async.waterfall(
		[
			cb1 => getMetadata.doIt(oTrace, oClient, sSchema, sNameDSO, cb1),
			(oMD, cb1) => {
				oMetadata = oMD;
				sActiveDataTabName = [
					'"',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
					'"."',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
					'.',
					oMD.activeData.name,
					'"'
				].join('');
				if (oMD.changeLog) {
					sChangeLogTabName = [
						'"',
						hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
						'"."',
						hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
						'.',
						hdbext.sqlInjectionUtils.escapeDoubleQuotes(oMD.changeLog.name),
						'"'
					].join('');
				}
				return cb1();
			},

			cb1 => getNewId.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				'OPERATION_REQUEST', sOperationDelete, sStatusRunning, false /*withCommit*/ , cb1
			),
			(opId, cb1) => {
				operationId = opId;
				oMsgStart.text = _.replace(oMsgStart.text, '&1', propagateDeletion);
				oMsgStart.text = _.replace(oMsgStart.text, '&2', sWhere);

				return writeMessage.doIt(
					oTrace, oClient2Connection, sSchema, sNameDSO, oMetadata,
					operationId, [oMsgStart], true /*withCommit*/ , cb1
				);
			},

			cb1 => !propagateDeletion ? cb1(null, 0) : getNewId.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				'ACTIVATION_REQUEST', null, null, false /*withCommit*/ , cb1
			),
			(actId, cb1) => {
				activationId = actId;
				return oClient.commit(cb1);
			},
			cb1 => {
				if (!propagateDeletion) {
					return cb1(null);
				} else if (!oMetadata.changeLog) {
					return cb1(new Error('Propagation of Deletion not possible as no Changelog exists'));
				}

				return writeAffectedRequests.doIt(
					oTrace, oClient, sSchema, sNameDSO, oMetadata,
					operationId, [activationId], false /*withCommit*/ , cb1
				);
			},

			cb1 => setOperationDetails.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				operationId, {
					'sWhere': sWhere
				}, false /*withCommit*/ , cb1
			),

			cb1 => !oMetadata.computeChecksum ? cb1(null, null) : checkChecksum.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				true /*forAT*/ , (oMetadata.changeLog ? true : false) /*forCL*/ , cb1
			),
			(oStored, oComputed, cb1) => updateOperationStatus.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				operationId, sStatusFinished, false /*withCommit*/ ,
				cb1
			),
			cb1 => !propagateDeletion ? cb1(null) : async.waterfall(
				[
					cb2 => {
						var sFieldlist = _.map(
							oMetadata.activeData.fields,
							field => [
								'"',
								hdbext.sqlInjectionUtils.escapeDoubleQuotes(field.name),
								'"'
							].join('')
						).join(',');

						var sSelectlist = _.map(
							oMetadata.activeData.fields,
							field => field.aggregation !== 'SUM' ? '"' + hdbext.sqlInjectionUtils.escapeDoubleQuotes(field.name) + '"' : ' - "' + hdbext.sqlInjectionUtils
							.escapeDoubleQuotes(field.name) + '"'
						).join(',');

						sSql =
							'insert into ' + sChangeLogTabName +
							'   ( ' + activationIdColName +
							'   , ' + recordNoColName +
							'   , ' + recordModeColName +
							'   , ' + sFieldlist + ' ) ' +
							'  select ' + activationId +
							'    , ROW_NUMBER() over ()' +
							'    , \'R\'' + // REVERSE_IMAGE
							'    , ' + sSelectlist +
							'  from ' + sActiveDataTabName +
							'  where ( ' + sWhere + ' )';
						return oClient.prepare(sSql, cb2);
					},
					(statement, cb2) => statement.exec([], cb2),
					(affectedRows, cb2) => cb2(null, affectedRows.length)
				],
				(err, rc) => {
					rowCountCL = rc;
					return cb1(err);
				}
			),
			cb1 => {
				sSql =
					'delete from ' + sActiveDataTabName +
					' where ' + sWhere;
				return oClient.prepare(sSql, cb1);
			},
			(statement, cb1) => statement.exec([], cb1),
			(affectedRows, cb1) => !propagateDeletion ? cb1(
				null, [{
					'tableName': sActiveDataTabName,
					'rowCount': affectedRows.length
				}]
			) : cb1(
				null, [{
					'tableName': sActiveDataTabName,
					'rowCount': affectedRows.length
				}, {
					'tableName': sChangeLogTabName,
					'rowCount': rowCountCL
				}]
			),
			(aR, cb1) => {
				aResult = aR;
				return cb1();
			},
			cb1 => !oMetadata.computeChecksum ? cb1(null, null) : updateChecksum.doIt(
				oTrace, oClient, sSchema, sNameDSO,
				oMetadata, operationId, true /*updateAT*/ , (oMetadata.changeLog ? true : false) /*updateCL*/ , cb1
			),
			(o, cb1) => {
				oMsgSuccess.text = _.replace(oMsgSuccess.text, '&1', activationId);
				oMsgSuccess.text = _.replace(
					oMsgSuccess.text, '&2',
					_.reduce(
						aResult,
						(rowCount, result) => rowCount + result.rowCount,
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
					if (sSql) {
						aMessages.push(new MsgErrorDetail('SQL: ' + sSql));
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
					operationId, sStatusFailed, true /*withCommit*/ , cb1)
			],
			err1 => err1 ? cb(err1) : cb(
				null, {
					'operationId': operationId,
					'changeLogId': activationId
				})
		) : cb(null, {
			'operationId': operationId,
			'changeLogId': activationId
		})
	);
};