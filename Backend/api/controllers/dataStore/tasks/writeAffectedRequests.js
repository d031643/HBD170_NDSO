/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var hdbext = require('@sap/hdbext');

exports.doIt = function(oTrace, oClient, sSchema, sNameDSO, oMetadata,
	operationId, aRequestIds, withCommit, cb) {

	oTrace.info('call of "writeAffectedRequests( ' + operationId + ', ' + '[' + (aRequestIds ? aRequestIds.join(',') : '') + '], ' +
		withCommit + ', ' + sSchema + ', ' + sNameDSO + ' )"');

	if (!sSchema) {
		cb(new Error('No Schema provided'));
		return;
	}
	if (!sNameDSO) {
		cb(new Error('No DataStore provided'));
		return;
	}
	if (!operationId) {
		cb(new Error('No operationID provided'));
		return;
	}

	var sAffectedRequestsTabName = [
		'"',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
		'"."',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
		'.',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(oMetadata.affectedRequests.name),
		'"'
	].join('');
	return async.waterfall(
		[
			cb1 => {
				var sSql =
					'insert into ' + sAffectedRequestsTabName +
					'  values ( ? , ? )';
				return oClient.prepare(sSql, cb1);
			},
			(statement, cb1) => statement.exec(aRequestIds.map(n => [operationId, n]), cb1),
			(a, cb1) => (!withCommit) ? cb1() : oClient.commit(cb1)
		],
		err => err ? async.waterfall(
			[
				cb1 => (!withCommit) ? cb1(err) : oClient.rollback(cb1)
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