/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var _ = require('lodash');
var hdbext = require('@sap/hdbext');
var getMetadata = require('./getMetadata');

exports.doIt = function(oTrace, oClient, sSchema, sNameDSO, maxRequestId, maxTimeStamp, cb) {

	oTrace.info('call of "getRequestsForCleanup( ' + maxRequestId + ',' + maxTimeStamp + ', ' + sSchema + ', ' + sNameDSO + ' )"');

	if (!sSchema) {
		return cb(new Error('No Schema provided'));
	}
	if (!sNameDSO) {
		return cb(new Error('No DataStore provided'));

	}

	var idColName = '"' + 'technicalKey.id' + '"';
	var typeColName = '"' + 'technicalAttributes.type' + '"';
	var requestIdColName = '"' + 'technicalKey.requestId' + '"';
	var operationIdColName = '"' + 'technicalKey.operationId' + '"';
	var operationColName = '"' + 'technicalAttributes.operation' + '"';
	var statusColName = '"' + 'technicalAttributes.status' + '"';
	var userNameColName = '"' + 'technicalAttributes.userName' + '"';
	var lastTimestampColName = '"' + 'technicalAttributes.lastTimestamp' + '"';
	var maxRequestColName = '"' + 'technicalAttributes.maxRequest' + '"';

	var sAffectedRequestsTabName = '';
	var sIdGeneratorTabName = '';
	var sOperationHistoryTabName = '';
	var sSubscribersTabName = '';

	async.waterfall(
		[
			cb1 => getMetadata.doIt(oTrace, oClient, sSchema, sNameDSO, cb1),
			(oMetadata, cb1) => {
				if (!oMetadata.changeLog) {
					var error = new Error('Cleanup not possible as no Changelog exists');
					error.doNotThrowError = true;
					cb1(error);
					return;
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
				sOperationHistoryTabName = [
					'"',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
					'"."',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
					'.',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(oMetadata.operationHistory.name),
					'"'
				].join('');
				sSubscribersTabName = [
					'"',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
					'"."',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
					'.',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(oMetadata.subscribers.name),
					'"'
				].join('');
				return cb1();
			},
			cb1 => {
				// get all ActivationId which are smaller than the specified max-value
				// and which are already extracted from all attached subscribers
				var sSql =
					'select ' +
					'    ID_GEN.' + idColName + ' as "activationId", ' +
					'    OP_HIST.' + userNameColName + ' as "userName", ' +
					'    TO_NVARCHAR( OP_HIST.' + lastTimestampColName + ', \'YYYYMMDDHH24MISS\' ) ' +
					'      as "timestamp" ' +
					'  from ' + sIdGeneratorTabName + ' ID_GEN ' +
					'  join ' + sAffectedRequestsTabName + ' AFF_REQ ' +
					'    on ID_GEN.' + idColName + ' = AFF_REQ.' + requestIdColName +
					'  join ' + sOperationHistoryTabName + ' OP_HIST ' +
					'     on AFF_REQ.' + operationIdColName + ' = OP_HIST.' + operationIdColName +
					'  where ID_GEN.' + typeColName + ' = \'ACTIVATION_REQUEST\'' +
					(maxRequestId ? '    and ID_GEN.' + idColName + ' <= ' + parseInt(maxRequestId) : '') +
					(maxTimeStamp ?
						'    and TO_NVARCHAR( OP_HIST.' + lastTimestampColName + ', \'YYYYMMDDHH24MISS\' ) ' +
						' <= ' + maxTimeStamp : '') +
					'    and OP_HIST.' + operationColName + ' = \'ACTIVATE\'' +
					'    and OP_HIST.' + statusColName + ' = \'FINISHED\'' +
					'    and not exists ( ' +
					'        select AFF_REQ1.' + requestIdColName +
					'          from ' + sAffectedRequestsTabName + ' AFF_REQ1 ' +
					'          join ' + sOperationHistoryTabName + ' OP_HIST1 ' +
					'            on AFF_REQ1.' + operationIdColName + ' = OP_HIST1.' + operationIdColName +
					'          where AFF_REQ1.' + requestIdColName + ' =  AFF_REQ.' + requestIdColName +
					'            and OP_HIST1.' + operationColName + ' in ( \'ROLLBACK\', \'CLEANUP_CHANGELOG\' ) ' +
					'            and OP_HIST1.' + statusColName + ' = \'FINISHED\' ) ' +
					'    and ID_GEN.' + idColName + ' <= ( ' +
					'        select coalesce( min( ' + maxRequestColName + ' ), ID_GEN.' + idColName + ' ) ' +
					'          from ' + sSubscribersTabName + ' ) ';

				return oClient.prepare(sSql, cb1);
			},
			(statement, cb1) => statement.exec([], cb1),
			(aRows, cb1) => cb1(
				null,
				_.map(
					aRows,
					row => {
						return {
							'requestId': row['activationId'],
							'userName': row['userName'],
							'timestamp': row['timestamp']
						};
					}
				)
			),
			(aRequests, cb1) => cb1(null, {
				result: aRequests
			})
		],

		(err, o) => {
			if (err) {
				if (err.doNotThrowError) {
					cb(null, {});
					return;
				} else {
					cb(err);
					return;
				}
			} else {
				cb(null, o);
				return;
			}
		});
};