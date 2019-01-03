/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var hdbext = require('@sap/hdbext');
var removeRunningOperations = require('./removeRunningOperations');

exports.doIt = function(oTrace, oClient, sSchema, sNameDSO, oMetadata,
	id, sStatus, withCommit, cb) {
	var sSql = null;

	oTrace.info('call of "updateOperationStatus( ' + id + ',' + sStatus + ', ' + sSchema + ', ' + sNameDSO + ' )"');

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
	var statusColName = '"' + 'technicalAttributes.status' + '"';
	var lastTimestampColName = '"' + 'technicalAttributes.lastTimestamp' + '"';

	var sOperationHistoryTabName = [
		'"',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
		'"."',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
		'.',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(oMetadata.operationHistory.name), '"'
	].join('');
	sSql =
		'update ' + sOperationHistoryTabName +
		'  set ' + statusColName + ' = \'' + sStatus + '\',' +
		'      ' + lastTimestampColName + ' = CURRENT_UTCTIMESTAMP ' +
		'  where ' + operationIdColName + ' = ?';
	return async.waterfall(
		[
			cb1 => oClient.prepare(sSql, cb1),
			(statement, cb1) => statement.exec([id], cb1),
			(affectedRows, cb1) => cb1(),
			cb1 => (sStatus === 'FINISHED') ? removeRunningOperations.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata, id, cb1
			) : cb1(),
			cb1 => !withCommit ? cb1() : oClient.commit(cb1)
		],
		err => err ? async.waterfall(
			[
				cb1 => {
					oTrace.log('SQL: ');
					oTrace.log(sSql);
					return cb1();
				},
				cb1 => (!withCommit) ? cb1(err) : oClient.rollback(cb1)
			],
			err1 => err1 ? async.waterfall(
				[
					cb1 => {
						oTrace.error('rollback: ');
						oTrace.error(err1.message);
						return cb1(err1);
					}
				],
				err2 => cb(err2)
			) : cb(err)
		) : cb()
	);
};