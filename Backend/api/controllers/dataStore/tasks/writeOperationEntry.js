/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var hdbext = require('@sap/hdbext');
var addRunningOperations = require('./addRunningOperations');

exports.doIt = function(oTrace, oClient, sSchema, sNameDSO, oMetaData,
	operationId, sOperation, sStatus, withCommit, cb) {

	oTrace.info('call of "writeOperationEntry( ' + operationId + ', ' + sOperation + ', ' + sStatus + ', ' + withCommit + ', ' + sSchema +
		', ' + sNameDSO + ' )"');

	if (!sSchema) {
		return cb(new Error('No Schema provided'));
	}
	if (!sNameDSO) {
		return cb(new Error('No DataStore provided'));
	}
	if (!operationId) {
		return cb(new Error('No operationID provided'));
	}

	var sOperationHistoryTabname = [
		'"',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
		'"."',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
		'.',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(oMetaData.operationHistory.name),
		'"'
	].join('');
	return async.waterfall(
		[
			cb1 => oClient.prepare(
				'insert into ' + sOperationHistoryTabname +
				' values (?, ?, CURRENT_USER, ' +
				'CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP' +
				', ? ,\'\',\'\',\'\' )', cb1
			),
			(statement, cb1) => statement.exec([operationId, sOperation, sStatus], cb1),
			(a, cb1) => (sStatus === 'RUNNING') ? addRunningOperations.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetaData, operationId, cb1
			) : cb1(),
			cb1 => (!withCommit) ? cb1() : oClient.commit(cb1)
		],
		err => err ? async.waterfall(
			[
				cb1 => !withCommit ? cb1(err) : oClient.rollback(cb1)
			],
			err1 => err1 ? cb(err1) : cb(err)
		) : cb(null)
	);
};