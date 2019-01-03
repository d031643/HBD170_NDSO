/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var hdbext = require('@sap/hdbext');
var _ = require('lodash');

exports.doIt = function(oTrace, oClient, sSchema, sNameDSO, oMetadata, operationId,
	aMessages, withCommit, cb) {
	var aData = null;
	var sSql = null;
	oTrace.info('call of "writeMessage( ' + operationId + ', ' + withCommit + ', ' + sSchema + ', ' + sNameDSO + ' )"');

	if (!sSchema) {
		return cb(new Error('No Schema provided'));
	}
	if (!sNameDSO) {
		return cb(new Error('No DataStore provided'));
	}
	if (!operationId) {
		return cb(new Error('No operationID provided'));
	}
	if (!aMessages || aMessages.length === 0) {
		return cb(new Error('No messages provided'));
	}

	var operationIdColName = '"' + 'technicalKey.operationId' + '"';
	var positColName = '"' + 'technicalKey.posit' + '"';

	var sLogMessagesTabName = [
		'"',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
		'"."',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
		'.',
		hdbext.sqlInjectionUtils.escapeDoubleQuotes(oMetadata.logMessages.name),
		'"'
	].join('');

	return async.waterfall(
		[
			cb1 => {
				_.each(aMessages, function(message) {
					switch (message.type) {
						case 'S':
						case 'I':
						case 'W':
							oTrace.info(message.number + ': ' + message.text);
							break;
						case 'E':
							oTrace.error(message.number + ': ' + message.text);
							break;
					}
				});
				return cb1();
			},
			cb1 => {
				sSql =
					'select max(' + positColName + ') as "max"' +
					'  from ' + sLogMessagesTabName +
					'  where ' + operationIdColName + ' = ?';
				//parseInt( operationId )
				return oClient.prepare(sSql, cb1);
			},
			(statement, cb1) => statement.exec([operationId], cb1),
			(aRows, cb1) => {
				var nextPosit = (aRows[0].max === null ? 0 : aRows[0].max + 1);
				return cb1(null, nextPosit);
			},
			(nextPosit, cb1) => {
				var recordNo = nextPosit;
				aData = _.map(aMessages, function(oMsg) {
					return [operationId, ++recordNo, oMsg.type, oMsg.number, oMsg.text];
				});
				sSql =
					'insert into ' + sLogMessagesTabName +
					'  values ( ?, ?, CURRENT_UTCTIMESTAMP, ?, ?, ?)';

				return oClient.prepare(sSql, cb1);
			},
			(statement, cb1) => statement.exec(aData, cb1),
			(a, cb1) => (!withCommit) ? cb1() : oClient.commit(cb1)
		],
		err => err ? async.waterfall(
			[
				cb1 => {
					oTrace.error(err);
					oTrace.log('SQL: ');
					oTrace.log(sSql);
					return cb1();
				},
				cb1 => (!withCommit) ? cb1(err) : oClient.rollback(cb1)
			],
			err1 => {
				if (err1) {
					oTrace.error('rollback: ');
					oTrace.error(err1.message);
					return cb(err1);
				}
				return cb(err);
			}
		) : cb()
	);
};