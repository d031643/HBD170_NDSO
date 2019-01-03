/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var hdbext = require('@sap/hdbext');
var getMetadata = require('./getMetadata');

exports.doIt = function(oTrace, oClient, oClient2Connection, sSchema, sNameDSO, sWhere, cb) {

	oTrace.info('call of "getRowcountWithFilter( ' + sWhere + ', ' +
		+sSchema + ', ' + sNameDSO + ' )"');

	if (!sSchema) {
		return cb(new Error('No Schema provided'));
	}
	if (!sNameDSO) {
		return cb(new Error('No DataStore provided'));
	}
	if (!sWhere) {
		return cb(new Error('No filter provided'));
	}

	var oMetadata = {};
	var activeDataTableName = '';

	return async.waterfall(
		[
			cb1 => getMetadata.doIt(oTrace, oClient, sSchema, sNameDSO, cb1),
			(oMD, cb1) => {
				oMetadata = oMD;
				return cb1();
			},
			cb1 => {
				activeDataTableName = [
					'"',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
					'"."',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
					'.',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(oMetadata.activeData.name),
					'"'
				].join('');
				var sSql =
					'select count(*) as "count"' +
					'  from ' + activeDataTableName +
					'  where ( ' + sWhere + ' )';

				return oClient.prepare(sSql, cb1);
			},
			(statement, cb1) => statement.exec([], cb1),
			(affectedRows, cb1) => cb1(
				null, {
					result: [{
						'tableName': activeDataTableName,
						'rowCount': affectedRows ? affectedRows[0].count : 0
					}]
				}
			)
		],
		cb
	);
};