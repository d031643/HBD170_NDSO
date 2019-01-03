/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var _ = require('lodash');
var hdbext = require('@sap/hdbext');
var getMetadata = require('./getMetadata');

exports.doIt = function(oTrace, oClient, sSchema, sNameDSO, cb) {
	var minRequestId = null;
	var aRows = null;
	var sSql = null;
	oTrace.info('call of "getRequestsForDeletion( ' + sSchema + ', ' + sNameDSO + ' )"');

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

	async.waterfall(
		[
			cb1 => getMetadata.doIt(oTrace, oClient, sSchema, sNameDSO, cb1),
			(oMetadata, cb1) => {
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
				//  determine minRequestId = { largest LoadId of all finished activations }
				sSql =
					'select max(' + requestIdColName + ') as "max" ' +
					'  from ' + sAffectedRequestsTabName + ' AFF_REQ ' +
					'  join ' + sIdGeneratorTabName + ' ID_GEN2 ' +
					'    on ID_GEN2.' + idColName + ' = AFF_REQ.' + requestIdColName +
					'  where ID_GEN2.' + typeColName + ' = \'LOAD_REQUEST\'' +
					'    and AFF_REQ.' + operationIdColName + ' = ' +
					'        (select coalesce(max(' + operationIdColName + '), -1)' +
					'           from ' + sOperationHistoryTabName +
					'           where ' + operationColName + ' = \'ACTIVATE\'' +
					'             and ' + statusColName + ' = \'FINISHED\' )';

				return oClient.prepare(sSql, cb1);
			},
			(statement, cb1) => statement.execute([], cb1),
			(aR, cb1) => cb1(null, (aR[0] && aR[0]['max'] ? aR[0]['max'] : -1)),

			function(n, cb1) {
				// get all LoadId >= min
				minRequestId = n;
				sSql =
					'select ' +
					'  ID_GEN.' + idColName + ' as "loadId", ' +
					'  OP_HIST.' + operationIdColName + ' as "operationId", ' +
					'  OP_HIST.' + userNameColName + ' as "userName", ' +
					'  TO_NVARCHAR( OP_HIST.' + lastTimestampColName + ', \'YYYYMMDDHH24MISS\' ) ' +
					'    as "timestamp" ' +
					'  from ' + sIdGeneratorTabName + ' ID_GEN ' +
					'  join ' + sAffectedRequestsTabName + ' AFF_REQ' +
					'    on ID_GEN.' + idColName + ' = AFF_REQ.' + requestIdColName +
					'  join ' + sOperationHistoryTabName + ' OP_HIST' +
					'    on AFF_REQ.' + operationIdColName + ' = OP_HIST.' + operationIdColName +
					'  where ID_GEN.' + typeColName + ' = \'LOAD_REQUEST\'' +
					'    and ID_GEN.' + idColName + ' > ' + parseInt(minRequestId) +
					'    and OP_HIST.' + operationColName + ' = \'LOAD\'' +
					'    and ( OP_HIST.' + statusColName + '= \'FINISHED\'' +
					'       or OP_HIST.' + statusColName + '= \'FAILED\' )';

				oClient.prepare(sSql, cb1);
			},
			(statement, cb1) => statement.exec([], cb1),
			// remove duplicates which might occur, if the same loadId is used in several operations.
			// For each set of duuplicates select record with largest operationId.
			(aR, cb1) => cb1(
				null,
				_.map(
					_.groupBy(
						aR,
						oRow => oRow['loadId']
					),
					oRow => _.reduce(
						oRow,
						(result, value) => (value['operationId'] >= result['operationId'] ? value : result), {
							'operationId': 0
						}
					)
				)
			),
			(aR, cb1) => {
				// filter out all 'deleted' requests
				// !!assumption: a LoadId must not be used for more than one operation!!
				aRows = aR;
				sSql =
					'select distinct ' + requestIdColName + ' as "requestId"' +
					'  from ' + sOperationHistoryTabName + ' OP_HIST ' +
					'  join ' + sAffectedRequestsTabName + ' AFF_REQ ' +
					'    on OP_HIST.' + operationIdColName + ' = AFF_REQ.' + operationIdColName +
					'  where AFF_REQ.' + requestIdColName + ' > ' + parseInt(minRequestId) +
					'    and OP_HIST.' + operationColName + ' = \'DELETE_REQUEST\'' +
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
							rowDel => rowDel['requestId'] === row['loadId']
						)
					)
				).map(
					row => {
						return {
							'requestId': row['loadId'],
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
		cb
	);
};