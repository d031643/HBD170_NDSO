/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';
var hdbext = require('@sap/hdbext');
var async = require('async');

exports.doIt = function(oTrace, oClient, sSchema, sNameDSO, oMetaData, operationId, cb) {

	oTrace.info('call of "removeRunningOperations( ' + operationId + ', ' + sSchema + ', ' + sNameDSO + ' )"');

	if (!sSchema) {
		return cb(new Error('No Schema provided'));
	}
	if (!sNameDSO) {
		return cb(new Error('No DataStore provided'));
	}
	if (!operationId) {
		return cb(new Error('No operationID provided'));
	}

	var sRunningOperationsTabname = [
		'"',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
		'"."',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
		'.',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(oMetaData.runningOperations.name),
		'"'
	].join('');
	var operationIdColName = '"' + 'technicalKey.operationId' + '"';

	var sSql =
		'delete from ' + sRunningOperationsTabname +
		' where ' + operationIdColName + ' = ?';

	async.waterfall(
		[
			cb1 => oClient.prepare(sSql, cb1),
			(statement, cb1) => statement.exec([operationId], cb1),
			(o, cb1) => cb1()
		],
		cb
	);
};