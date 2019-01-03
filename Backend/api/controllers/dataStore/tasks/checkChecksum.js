/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var hdbext = require('@sap/hdbext');
var computeChecksum = require('./computeChecksum');

exports.doIt = function(oTrace, oClient, sSchema, sNameDSO, oMetadata, forAT, forCL, cb) {
	var sSql = null;

	oTrace.info('call of "checkChecksum( forAT:' + forAT + ', forCL:' + forCL + sSchema + ', ' + sNameDSO + ' )"');

	if (!sSchema) {
		return cb(new Error('No Schema provided'));
	}
	if (!sNameDSO) {
		return cb(new Error('No DataStore provided'));
	}

	var oStored = {
		checksumAT: '',
		checksumCL: ''
	};

	var oComputed = {
		checksumAT: '',
		checksumCL: ''
	};

	var operationIdColName = '"' + 'technicalKey.operationId' + '"';
	var operationColName = '"' + 'technicalAttributes.operation' + '"';
	var statusColName = '"' + 'technicalAttributes.status' + '"';
	var checksumATColName = '"' + 'technicalAttributes.checksumAT' + '"';
	var checksumCLColName = '"' + 'technicalAttributes.checksumCL' + '"';

	var sOperationHistoryTabName = [
		'"',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
		'"."',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
		'.',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(oMetadata.operationHistory.name),
		'"'
	].join('');

	async.waterfall(
		[
			// get last operation, which might modify the checksum
			// AT: 'ACTIVATE', 'ROLLBACK', 'DELETE_WITH_FILTER'
			// CL: 'ACTIVATE', 'ROLLBACK', 'DELETE_WITH_FILTER', 'CLEANUP_CHANGELOG'

			cb1 => {
				sSql = 'select ' + checksumATColName +
					'  from ' + sOperationHistoryTabName +
					'  where ' + operationIdColName + ' = ( select max( ' + operationIdColName + ' ) ' +
					'      from ' + sOperationHistoryTabName +
					'      where ' + operationColName +
					'            in ( \'ACTIVATE\', \'ROLLBACK\', \'DELETE_WITH_FILTER\' ) ' +
					'        and ' + statusColName + ' = \'FINISHED\' )';
				return oClient.prepare(sSql, cb1);
			},
			(statement, cb1) => statement.exec([], cb1),
			(aRows, cb1) => {
				if (aRows.length === 0) {
					return cb1(null, '');
				} else if (aRows.length > 1) {
					return cb1(
						new Error('Failed to select metadata: (rows: ' + aRows.length + ', sql: ' + sSql)
					);
				}
				return cb1(
					null,
					aRows[0]['technicalAttributes.checksumAT']
				);
			},
			(checksum, cb1) => {
				oStored.checksumAT = checksum;
				return cb1(null);
			},
			cb1 => {
				sSql = 'select ' + checksumCLColName +
					'  from ' + sOperationHistoryTabName +
					'  where ' + operationIdColName + ' = ( select max( ' + operationIdColName + ' ) ' +
					'      from ' + sOperationHistoryTabName +
					'      where ' + operationColName +
					'            in ( \'ACTIVATE\', \'ROLLBACK\'' +
					'               , \'DELETE_WITH_FILTER\', \'CLEANUP_CHANGELOG\' ) ' +
					'        and ' + statusColName + ' = \'FINISHED\' )';
				return oClient.prepare(sSql, cb1);
			},
			(statement, cb1) => statement.exec([], cb1),
			(aRows, cb1) => {
				if (aRows.length === 0) {
					return cb1(null, '');
				} else if (aRows.length > 1) {
					cb1(
						new Error('Failed to select metadata: (rows: ' + aRows.length + ', sql: ' + sSql)
					);
				}
				return cb1(
					null,
					aRows[0]['technicalAttributes.checksumCL']
				);
			},
			(checksum, cb1) => {
				oStored.checksumCL = checksum;
				return cb1(null);
			},
			cb1 => (!forAT) ? cb1(null, null) : computeChecksum.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata, 'AT', cb1
			),
			(checksum, cb1) => {
				oComputed.checksumAT = checksum;
				return cb1(null);
			},
			cb1 => (!forCL) ? cb1(null, null) : computeChecksum.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata, 'CL', cb1
			),
			(checksum, cb1) => {
				oComputed.checksumCL = checksum;
				return cb1(null);
			},
			cb1 => {
				if (forAT && oStored.checksumAT && oStored.checksumAT !== oComputed.checksumAT) {
					return cb1(
						new Error(
							[
								'Checksum mismatch for ACTIVE-DATA-table: "',
								oStored.checksumAT,
								'" != "',
								oComputed.checksumAT,
								'"'
							].join('')
						)
					);
				} else if (
					forCL && oStored.checksumCL && oStored.checksumCL !== oComputed.checksumCL
				) {
					return cb1(
						new Error(
							[
								'Checksum mismatch for CHANGELOG-table: ',
								oStored.checksumCL,
								'!==',
								oComputed.checksumCL
							].join('')
						)
					);
				}
				return cb1(null);
			}
		],
		err => cb(err, oStored, oComputed)
	);
};