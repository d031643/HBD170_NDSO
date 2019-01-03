/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var hdbext = require('@sap/hdbext');

exports.doIt = function(oTrace, oClient, sSchema, sNameDSO, oMetadata,
	aActivationIds, withCommit, cb) {

	oTrace.info('call of "deleteAggregationHistory( [' + (aActivationIds ? aActivationIds.join(',') : '') + '], ' + withCommit + ', ' +
		sSchema + ', ' + sNameDSO + ')"');

	if (!sSchema) {
		return cb(new Error('No Schema provided'));
	}
	if (!sNameDSO) {
		return cb(new Error('No DataStore provided'));
	}
	if (!aActivationIds || aActivationIds.length === 0) {
		return cb(new Error('No activation Id provided '));
	}

	var requestIdColName = '"' + 'technicalKey.requestId' + '"';

	var sAggregationHistoryTabName = [
		'"',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
		'"."',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
		'.',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(oMetadata.aggregationHistory.name),
		'"'
	].join('');

	async.waterfall(
		[
			cb1 => oClient.prepare(
				'delete from ' + sAggregationHistoryTabName + // escaped earlier
				'  where ' + requestIdColName + ' = ?', cb1
			),
			(statement, cb1) => statement.exec(aActivationIds.map(n => [n]), cb1),
			cb1 => !withCommit ? cb1() : oClient.commit(cb1)
		],
		err => err ? async.waterfall(
			[
				cb1 => !withCommit ? cb1(err) : oClient.rollback(cb1)
			],
			err1 => err1 ? cb(err1) : cb(err)
		) : cb(null)
	);
};