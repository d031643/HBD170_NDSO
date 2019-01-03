/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var _ = require('lodash');
var hdbext = require('@sap/hdbext');

var getMetadata = require('./getMetadata');

exports.doIt = function(oTrace, oClient, sSchema, sNameDSO, operationId, cb) {

	oTrace.info('call of "getLogForOperation( ' + operationId + ', ' + sSchema + ', ' + sNameDSO + ')"');

	if (!sSchema) {
		cb(new Error('No Schema provided'));
		return;
	}
	if (!sNameDSO) {
		return cb(new Error('No DataStore provided'));
	}
	if (!operationId) {
		return cb(new Error('No operationID provided'));
	}

	var operationIdColName = '"' + 'technicalKey.operationId' + '"';
	var positColName = '"' + 'technicalKey.posit' + '"';
	var timestampColName = '"' + 'technicalAttributes.timestamp' + '"';
	var msgTypeColName = '"' + 'technicalAttributes.msgType' + '"';
	var msgNumberColName = '"' + 'technicalAttributes.msgNumber' + '"';
	var msgTextColName = '"' + 'technicalAttributes.msgText' + '"';

	var sLogMessagesTabName = '';

	async.waterfall(
		[
			cb1 => getMetadata.doIt(oTrace, oClient, sSchema, sNameDSO, cb1),
			(oMetadata, cb1) => {
				sLogMessagesTabName = [
					'"',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
					'"."',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
					'.',
					oMetadata.logMessages.name,
					'"'
				].join('');
				return cb1(null, oMetadata);
			},
			(oMetadata, cb1) => {
				var sSql =
					'select ' +
					positColName + ', ' +
					'TO_NVARCHAR( ' + timestampColName + ', \'YYYYMMDDHH24MISS\' ) ' +
					'as ' + timestampColName + ' ,' +
					msgTypeColName + ',' +
					msgNumberColName + ',' +
					msgTextColName +
					' from ' + sLogMessagesTabName +
					' where ' + operationIdColName + ' =  ?' +
					' order by ' + positColName;

				return oClient.prepare(sSql, cb1);
			},
			(statement, cb1) => statement.exec([operationId], cb1),
			(aRows, cb1) => {
				var aRequests = aRows.map(function(oRow) {
					var oRequest = {};
					for (var prop in oRow) {
						var aName = _.split(prop, '.');
						if (aName[aName.length - 1] === 'msgText') {
							oRequest[
								aName[aName.length - 1]
							] = !oRow[prop] ? '<Missing message text>' : oRow[prop].toString();
						} else {
							oRequest[aName[aName.length - 1]] = oRow[prop];
						}
					}
					return oRequest;
				});
				return cb1(null, {
					result: aRequests
				});
			}
		],
		cb
	);
};