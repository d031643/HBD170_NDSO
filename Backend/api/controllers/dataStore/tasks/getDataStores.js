/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');

exports.doIt = function(oTrace, oClient, sSchema, cb) {
	var sSql = 'select artifact_name' +
		'  from cds_annotation_values' +
		'  where schema_name = ?' +
		'    and annotation_name = \'DataWarehouse.DataStore::Annotations\'' +
		'    and value like \'%"isDSO": "true"%\' order by artifact_name';
	oTrace.info('call of "getDataStores( ' + sSchema + ' )"');

	if (!sSchema) {
		cb(new Error('No Schema provided'));
		return;
	}

	async.waterfall(
		[
			cb1 => oClient.prepare(sSql, cb1),
			(statement, cb1) => statement.exec([sSchema], cb1),
			(aRows, cb1) => cb1(null, {
				result: aRows.map(aRow => aRow['ARTIFACT_NAME'])
			})
		],
		cb
	);
};