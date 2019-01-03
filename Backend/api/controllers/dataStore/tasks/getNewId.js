/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var hdbext = require('@sap/hdbext');
var writeOperationEntry = require('./writeOperationEntry');

exports.doIt = function(oTrace, oClient, sSchema, sNameDSO, oMetadata,
	sRequestType, sOperation, sStatus, withCommit, cb) {

	var newId = null;

	oTrace.info('call of "getNewId( ' + sRequestType + ', ' + sOperation + ', ' + sStatus + ', ' + withCommit + ',' + sSchema + ', ' +
		sNameDSO + ' )"');

	if (!sSchema) {
		return cb(new Error('No Schema provided'));

	}
	if (!sNameDSO) {
		return cb(new Error('No DataStore provided'));
	}
	if (!sRequestType) {
		return cb(new Error('No Request type provided'));
	}

	var idColumnName = 'technicalKey.id';

	var sIdGeneratorTabName = [
		'"',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
		'"."',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
		'.',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(oMetadata.idGenerator.name),
		'"'
	].join('');

	return async.waterfall(
		[
			cb1 => oClient.prepare('select count(*) as "count" from ' + sIdGeneratorTabName, cb1),
			(statement, cb1) => statement.exec([], cb1),
			(aRows, cb1) => cb1(null, aRows[0].count),
			(rowCount, cb1) => {
				var sSql = '';
				if (rowCount === 0) {
					sSql = 'insert into ' + sIdGeneratorTabName + 'values (1, ?)';
				} else {
					sSql = 'insert into ' + sIdGeneratorTabName +
						' ( select max("' + idColumnName + '") + 1, ?' +
						' from ' + sIdGeneratorTabName + ')';
				}
				return oClient.prepare(sSql, cb1);
			},
			(statement, cb1) => statement.exec([sRequestType], cb1),
			(affectedRows, cb1) => cb1(affectedRows !== 1 ? new Error('Update of "idGenerator" failed') : null),
			cb1 => oClient.prepare(
				'select max("' + idColumnName + '") as "max" from ' + sIdGeneratorTabName,
				cb1
			),
			(statement, cb1) => statement.exec([], cb1),
			(aRows, cb1) => (aRows.length !== 1) ? cb1(
				new Error('select max(id) failed')) : cb1(null, aRows[0].max),
			(nId, cb1) => {
				newId = nId;
				return (!sOperation) ? cb1(
					null
				) : writeOperationEntry.doIt(
					oTrace, oClient, sSchema, sNameDSO, oMetadata,
					newId, sOperation, sStatus, withCommit, cb1
				);
			},
			cb1 => !withCommit ? cb1(null) : oClient.commit(cb1)
		],
		err => err ? async.waterfall(
			[
				cb1 => (!withCommit) ? cb(err) : oClient.rollback(cb1)
			],
			(err1) => err1 ? cb(err1) : cb(err)
		) : (!newId ? cb(new Error('Failed to retrieve id')) : cb(null, newId))
	);
};