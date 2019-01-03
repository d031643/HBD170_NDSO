/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var hdbext = require('@sap/hdbext');
var _ = require('lodash');
var getMetadata = require('./getMetadata');

exports.doIt = function(oTrace, oClient, sSchema, sNameDSO, minRequestId, cb) {
	var maxRequestId = null;
	var aRows = null;

	oTrace.info('call of "getRequestsForRollback( ' + minRequestId + ', ' + sSchema + ', ' + sNameDSO + ' )"');

	if (!sSchema) {
		cb(new Error('No Schema provided'));
		return;
	}
	if (!sNameDSO) {
		cb(new Error('No DataStore provided'));
		return;
	}

	var idColName = '"' + 'technicalKey.id' + '"';
	var typeColName = '"' + 'technicalAttributes.type' + '"';
	var requestIdColName = '"' + 'technicalKey.requestId' + '"';
	var operationIdColName = '"' + 'technicalKey.operationId' + '"';
	var operationColName = '"' + 'technicalAttributes.operation' + '"';
	var statusColName = '"' + 'technicalAttributes.status' + '"';
	var userNameColName = '"' + 'technicalAttributes.userName' + '"';
	var lastTimestampColName = '"' + 'technicalAttributes.lastTimestamp' + '"';

	var sAffectedRequestsTabName = '';
	var sIdGeneratorTabName = '';
	var sOperationHistoryTabName = '';

	return async.waterfall(
		[
			cb1 => getMetadata.doIt(oTrace, oClient, sSchema, sNameDSO, cb1),
			(oMetadata, cb1) => {
				if (!oMetadata.changeLog) {
					var error = new Error('Rollback not possible as no Changelog exists');
					error.doNotThrowError = true;
					return cb1(error);
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
				return cb1();
			},
			cb1 => {
				// determine minRequestId
				// ensure, that min requestId is larger than largest 'cleanup'-request
				var sSql =
					'select max(' + requestIdColName + ') as "max" ' +
					'  from ' + sAffectedRequestsTabName + ' AFF_REQ ' +
					'  join ' + sOperationHistoryTabName + ' OP_HIST ' +
					'    on OP_HIST.' + operationIdColName + ' = AFF_REQ.' + operationIdColName +
					'  where OP_HIST.' + operationColName + ' = \'CLEANUP_CHANGELOG\'' +
					'    and OP_HIST.' + statusColName + ' = \'FINISHED\'';
				return oClient.prepare(sSql, cb1);
			},
			(statement, cb1) => statement.exec([], cb1),
			(aR, cb1) => {
				if (!minRequestId) {
					minRequestId = -1;
				}
				if (aR && aR[0] && aR[0]['max'] > minRequestId) {
					minRequestId = aR[0]['max'] + 1;
				}
				return cb1();
			},
			cb1 => {
				//  determine maxRequestId = { largest requestId of all finished activations }
				var sSql =
					'select max(' + requestIdColName + ') as "max" ' +
					'  from ' + sAffectedRequestsTabName + ' AFF_REQ ' +
					'  join ' + sOperationHistoryTabName + ' OP_HIST ' +
					'    on OP_HIST.' + operationIdColName + ' = AFF_REQ.' + operationIdColName +
					'  where OP_HIST.' + operationColName + ' in ( \'ACTIVATE\', \'DELETE_WITH_FILTER\' )' +
					'    and OP_HIST.' + statusColName + ' = \'FINISHED\'';

				return oClient.prepare(sSql, cb1);
			},
			(aR, cb1) => cb1(null, (aR[0] && aR[0]['max'] ? aR[0]['max'] : -1)),
			(n, cb1) => {
				maxRequestId = n;
				// get all ActivationId between min and max
				var sSql =
					'select ' +
					'  ID_GEN.' + idColName + ' as "activationId", ' +
					'  OP_HIST.' + operationIdColName + ' as "operationId", ' +
					'  OP_HIST.' + userNameColName + ' as "userName", ' +
					'  TO_NVARCHAR( OP_HIST.' + lastTimestampColName + ', \'YYYYMMDDHH24MISS\' ) ' +
					'    as "timestamp" ' +
					'  from ' + sIdGeneratorTabName + ' ID_GEN ' +
					'  join ' + sAffectedRequestsTabName + ' AFF_REQ' +
					'    on ID_GEN.' + idColName + ' = AFF_REQ.' + requestIdColName +
					'  join ' + sOperationHistoryTabName + ' OP_HIST' +
					'    on AFF_REQ.' + operationIdColName + ' = OP_HIST.' + operationIdColName +
					'  where ID_GEN.' + typeColName + ' = \'ACTIVATION_REQUEST\'' +
					'    and OP_HIST.' + operationColName + ' in ( \'ACTIVATE\', \'DELETE_WITH_FILTER\' )' +
					'    and OP_HIST.' + statusColName + ' = \'FINISHED\'' +
					'    and ID_GEN.' + idColName + ' <= ' + parseInt(maxRequestId) +
					'    and ID_GEN.' + idColName + ' >= ' + parseInt(minRequestId);

				return oClient.prepare(sSql, cb1);
			},
			(statement, cb1) => statement.exec([], cb1),
			(aR, cb1) => {
				aRows = aR;
				// filter out all 'rolled back' requests
				var sSql =
					'select distinct ' + requestIdColName + ' as "requestId"' +
					'  from ' + sOperationHistoryTabName + ' OP_HIST ' +
					'  join ' + sAffectedRequestsTabName + ' AFF_REQ ' +
					'    on OP_HIST.' + operationIdColName + ' = AFF_REQ.' + operationIdColName +
					'  where AFF_REQ.' + requestIdColName + ' <= ' + parseInt(maxRequestId) +
					'    and AFF_REQ.' + requestIdColName + ' >= ' + parseInt(minRequestId) +
					'    and OP_HIST.' + operationColName + ' = \'ROLLBACK\'' +
					'    and OP_HIST.' + statusColName + ' = \'FINISHED\'';

				return oClient.prepare(sSql, cb1);
			},
			(aRowsDel, cb1) => cb1(
				null,
				_.filter(
					aRows,
					row => !(
						_.find(
							aRowsDel,
							rowDel => rowDel['requestId'] === row['activationId']
						)
					)
				).map(
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
					return cb(null, []);
				} else {
					return cb(err);
				}
			} else {
				return cb(null, o);
			}
		}
	);
};