/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var _ = require('lodash');
var hdbext = require('@sap/hdbext');

exports.doIt = function(oTrace, oClient, sSchema, sNameDSO, oMetadata, source, cb) {
	var aFields = null;
	var sTableName = null;
	oTrace.info('call of "computeChecksum( ' + source + ', ' + sSchema + ', ' + sNameDSO + ' )"');

	if (!sSchema) {
		return cb(new Error('No Schema provided'));
	}
	if (!sNameDSO) {
		return cb(new Error('No DataStore provided'));
	}
	if (source === 'AT') {
		aFields = oMetadata.activeData.fields;
		sTableName = [
			'"',
			hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
			'"."',
			hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
			'.',
			oMetadata.activeData.name,
			'"'
		].join('');
	} else if (source === 'CL') {
		if (!oMetadata.changeLog) {
			return cb(new Error('Computation of Checksum not possible as no Changelog exists'));
		}

		aFields = oMetadata.changeLog.fields;
		sTableName = [
			'"',
			hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
			'"."',
			hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
			'.',
			hdbext.sqlInjectionUtils.escapeDoubleQuotes(oMetadata.changeLog.name),
			'"'
		].join('');
	} else {
		return cb(new Error('Invalid source provided'));
	}

	var aFieldlistKey = _.map(
		_.filter(
			aFields,
			field => field.isKey ? true : false
		),
		field => '"' + hdbext.sqlInjectionUtils.escapeDoubleQuotes(field.name) + '"'
	);

	var aFieldlistNonKey = _.map(
		_.filter(
			aFields,
			field => field.isKey ? false : true
		),
		field => '"' + hdbext.sqlInjectionUtils.escapeDoubleQuotes(field.name) + '"'
	);
	return async.waterfall(
		[
			cb1 => oClient.prepare(
				[
					'select ifnull(to_varchar( ( hash_sha256( to_binary( string_agg( HASH ) ) )) ), \'\') as H from ',
					'(select hash_sha256( to_binary( to_varchar(',
					aFieldlistKey.join(' || '), // escaped earlier above
					(aFieldlistNonKey.length !== 0 ? ' || ' + aFieldlistNonKey.join(' || ') : ''),
					' ))) as "HASH" ',
					' from ',
					sTableName, //escaped earlier
					' order by ',
					aFieldlistKey.join(','),
					')'
				].join(''), cb1),
			(statement, cb1) => statement.exec([], cb1),
			(aRows, cb1) => cb1(null, aRows[0].H)
		],
		cb
	);
};