/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var hdbext = require('@sap/hdbext');
var _ = require('lodash');

exports.doIt = function(oTrace, oClient, sSchema, sNameDSO, oMetadata,
	activationId, withCommit, cb) {

	oTrace.info('call of "writeAggregationHistory( ' + activationId + ', ' + withCommit + ', ' + sSchema + ', ' + sNameDSO + ' )"');

	if (!sSchema) {
		return cb(new Error('No Schema provided'));
	}
	if (!sNameDSO) {
		return cb(new Error('No DataStore provided'));
	}
	if (!activationId) {
		return cb(new Error('No activationID provided'));
	}

	var sAggregationHistoryTabName = [
		'"',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
		'"."',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
		'.',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(oMetadata.aggregationHistory.name),
		'"'
	].join('');

	var aData = _.flatten(
		_.map(
			oMetadata.activationQueues,
			oActivationQueue => _.map(
				_.filter(
					oActivationQueue.fields,
					row => row.aggregation !== 'MOV'
				),
				row => [
					activationId,
					oActivationQueue.name,
					row.name,
					row.aggregation
				]
			)
		)
	);
	if (!aData || aData.length === 0) {
		// only 'default' aggregation.
		return cb();
	}

	return async.waterfall(
		[
			cb1 => oClient.prepare(
				'insert into ' + sAggregationHistoryTabName + ' values (?,?,?,?)',
				cb1
			),
			(statement, cb1) => statement.exec(aData, cb1),
			(a, cb1) => !withCommit ? cb1() : oClient.commit(cb1)
		],

		err => err ? async.waterfall(
			[
				cb1 => !withCommit ? cb1() : oClient.rollback(cb1)
			],
			err1 => {
				if (err1) {
					oTrace.error('rollback: ');
					oTrace.error(err1.message);
					return cb(err1);
				} else {
					return cb(err);
				}
			}
		) : cb(null)
	);
};