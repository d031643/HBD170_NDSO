/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var hdbext = require('@sap/hdbext');

exports.doIt = function(oTrace, oClient, sSchema, sNameDSO, oMetadata,
	id, oOperationDetails, withCommit, cb) {

	oTrace.info('call of "setOperationDetails( ' + id + ',' + oOperationDetails + ', ' + sSchema + ', ' + sNameDSO + ' )"');

	if (!sSchema) {
		return cb(new Error('No Schema provided'));
	}
	if (!sNameDSO) {
		return cb(new Error('No DataStore provided'));
	}
	if (!id) {
		return cb(new Error('No operationID provided'));
	}

	var operationIdColName = '"' + 'technicalKey.operationId' + '"';
	var operationDetailColName = '"' + 'technicalAttributes.operationDetails' + '"';
	var lastTimestampColName = '"' + 'technicalAttributes.lastTimestamp' + '"';

	var sOperationHistoryTabName = [
		'"',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
		'"."',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
		'.',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(oMetadata.operationHistory.name),
		'"'
	].join('');
	return async.waterfall(
		[
			cb1 => {
				var sSql =
					'update ' + sOperationHistoryTabName +
					'  set ' + operationDetailColName + ' = ' +
					' \'' + JSON.stringify(oOperationDetails).replace(
						/\'/g, '\'\'') + '\',' +
					' ' + lastTimestampColName + ' = CURRENT_UTCTIMESTAMP ' +
					'  where ' + operationIdColName + ' = ?';
				return oClient.prepare(sSql, cb1);
			},
			(statement, cb1) => statement.exec([id], cb1),
			(affectedRows, cb1) => cb1((affectedRows !== 1) ? new Error(
				'no matching record found'
			) : null),
			cb1 => !withCommit ? cb1() : oClient.commit(cb1)
		],
		err => (err) ? async.waterfall(
			[
				cb1 => !withCommit ? cb1(err) : oClient.rollback(cb1)
			],
			err1 => {
				if (err1) {
					oTrace.error('rollback: ');
					oTrace.error(err1.message);
					return cb(err1);
				}
				return cb(err);
			}
		) : cb()
	);
};