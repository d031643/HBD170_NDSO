/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var _ = require('lodash');
var hdbext = require('@sap/hdbext');
var getMetadata = require('./getMetadata');

exports.doIt = function(oTrace, oClient, sSchema, sNameDSO, requestId, cb) {
	var aOperations = null;
	oTrace.info('call of "getOperationsForRequest( ' + requestId + ', ' + sSchema + ', ' + sNameDSO + ' )"');

	if (!sSchema) {
		cb(new Error('No Schema provided'));
		return;
	}
	if (!sNameDSO) {
		cb(new Error('No DataStore provided'));
		return;
	}
	if (!requestId) {
		cb(new Error('No Request provided'));
		return;
	}

	var requestIdColName = '"' + 'technicalKey.requestId' + '"';
	var operationIdColName = '"' + 'technicalKey.operationId' + '"';
	var operationColName = '"' + 'technicalAttributes.operation' + '"';
	var userNameColName = '"' + 'technicalAttributes.userName' + '"';
	var lastTimestampColName = '"' + 'technicalAttributes.lastTimestamp' + '"';
	var statusColName = '"' + 'technicalAttributes.status' + '"';

	var sAffectedRequestsTabName = '';
	var sOperationHistoryTabName = '';

	return async.waterfall(
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
				sOperationHistoryTabName = [
					'"',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
					'"."',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
					'.',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(oMetadata.operationHistory.name),
					'"'
				].join('');

				var sSql =
					'select ' +
					'  OP_HIST.' + operationIdColName + ', ' +
					'  OP_HIST.' + operationColName + ', ' +
					'  OP_HIST.' + statusColName + ', ' +
					'  OP_HIST.' + userNameColName + ', ' +
					'  TO_NVARCHAR( OP_HIST.' + lastTimestampColName + ', \'YYYYMMDDHH24MISS\' ) ' +
					'  as ' + lastTimestampColName +
					' from ' + sOperationHistoryTabName + ' OP_HIST ' +
					' join ' + sAffectedRequestsTabName + ' AFF_REQ ' +
					'   on OP_HIST.' + operationIdColName + ' = AFF_REQ.' + operationIdColName +
					' where AFF_REQ.' + requestIdColName + ' = ' + parseInt(requestId) +
					' order by OP_HIST.' + lastTimestampColName;
				return oClient.prepare(sSql, cb1);
			},
			(statement, cb1) => statement.exec([], cb1),
			(aRows, cb1) => {
				aOperations = aRows.map(function(oRow) {
					var oOperation = {};
					for (var prop in oRow) {
						var aName = _.split(prop, '.');
						oOperation[aName[aName.length - 1]] = oRow[prop];
					}
					return oOperation;
				});

				if (aOperations.length === 0) {
					//cb1( new Error( 'No Operation found for request ' + requestId ) );
					cb1(null, []);
					return;
				}

				// create string with all relevant operations
				var sOperationIds = _.map(aOperations, function(row) {
					return parseInt(row['operationId']);
				}).join(',');

				// read all affected requestIds for given operations
				var sSql =
					'select ' +
					operationIdColName + ' as "operationId", ' +
					requestIdColName + ' as "requestId" ' +
					'  from ' + sAffectedRequestsTabName +
					'  where ' + operationIdColName + ' in (' + sOperationIds + ')';

				return oClient.prepare(sSql, cb1);
			},
			(statement, cb1) => statement.exec([], cb1),
			(aAffectedRequests, cb1) => {
				var aR = _.groupBy(aAffectedRequests, function(request) {
					return request['operationId'];
				});

				_.forEach(aOperations, function(operation) {
					operation.affectedRequests = _.map(aR[operation.operationId], function(affectedRequest) {
						return affectedRequest.requestId;
					});
				});

				cb1(null);
				return;
			}
		],
		err => cb(err, {
			result: aOperations
		})
	);
};