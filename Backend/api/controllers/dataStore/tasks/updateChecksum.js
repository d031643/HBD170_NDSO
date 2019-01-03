/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var hdbext = require('@sap/hdbext');
var computeChecksum = require('./computeChecksum');

exports.doIt = function(oTrace, oClient, sSchema, sNameDSO, oMetadata, operationId, updateAT, updateCL, cb) {

	oTrace.info('call of "updateChecksum( ' + operationId + ', updateAT:' + updateAT + ', updateCL:' + updateCL + sSchema + ', ' + sNameDSO +
		' )"');

	if (!sSchema) {
		cb(new Error('No Schema provided'));
		return;
	}
	if (!sNameDSO) {
		cb(new Error('No DataStore provided'));
		return;
	}

	if (!updateAT && !updateCL) {
		cb();
		return;
	}

	var operationIdColName = '"' + 'technicalKey.operationId' + '"';
	var checksumATColName = '"' + 'technicalAttributes.checksumAT' + '"';
	var checksumCLColName = '"' + 'technicalAttributes.checksumCL' + '"';
	var lastTimestampColName = '"' + 'technicalAttributes.lastTimestamp' + '"';

	var sOperationHistoryTabName = [
		'"',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
		'"."',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
		'.',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(oMetadata.operationHistory.name),
		'"'
	].join('');

	var o = {
		'checksumAT': '',
		'checksumCL': ''
	};

	async.waterfall(
		[
			cb1 => (!updateAT) ? cb1(null, {}) : computeChecksum.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata, 'AT', cb1
			),
			(checksum, cb1) => {
				o.checksumAT = checksum;
				return cb1();
			},
			(cb1) => (!updateCL) ? cb1(null, {}) : computeChecksum.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata, 'CL', cb1
			),
			(checksum, cb1) => {
				o.checksumCL = checksum;
				return cb1();
			},
			cb1 => {
				var aSet = [];
				if (updateAT) {
					aSet.push(checksumATColName + ' = \'' + o.checksumAT + '\'');
				}
				if (updateCL) {
					aSet.push(checksumCLColName + ' = \'' + o.checksumCL + '\'');
				}
				var sSql =
					'update ' + sOperationHistoryTabName +
					'  set ' + aSet.join(',') +
					'      ,' + lastTimestampColName + ' = CURRENT_UTCTIMESTAMP ' +
					'  where ' + operationIdColName + ' =  ?';
				return oClient.prepare(sSql, cb1);
			},
			(statement, cb1) => statement.exec([operationId], cb1),
			(a, cb1) => cb1(null)
		],
		err => cb(err, o)
	);
};