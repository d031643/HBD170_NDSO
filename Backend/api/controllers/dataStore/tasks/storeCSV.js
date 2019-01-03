/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var hdbext = require('@sap/hdbext');
var _ = require('lodash');
var getMetadata = require('./getMetadata');
var getNewId = require('./getNewId');
var updateOperationStatus = require('./updateOperationStatus');
var writeMessage = require('./writeMessage');
var writeAffectedRequests = require('./writeAffectedRequests');

exports.doIt = function(oTrace, oClient, oClient2Connection, sSchema, sNameDSO,
	sActivationQueue, data, withHeaderLine, cb) {
	var operationId = null;
	var loadId = null;
	var rowCount = null;
	var aData = [];
	var sSql = null;
	var aFieldList = null;

	oTrace.info('call of "storeCSV( ' + sActivationQueue + ',' + withHeaderLine + ',' +
		sSchema + ', ' + sNameDSO + ' )"');

	if (!sSchema) {
		return cb(new Error('No Schema provided'));
	}
	if (!sNameDSO) {
		return cb(new Error('No DataStore provided'));
	}
	if (!sActivationQueue) {
		return cb(new Error('No activation queue table provided'));
	}
	if (!data) {
		return cb(new Error('No data provided'));
	}

	// use constants from annotations
	var sOperationLoad = 'LOAD';
	var sStatusRunning = 'RUNNING';
	var sStatusFinished = 'FINISHED';
	var sStatusFailed = 'FAILED';

	var sLoadIdFieldname = '"' + 'technicalKey.loadId' + '"';
	var sRecordNoFieldname = '"' + 'technicalKey.recordNo' + '"';

	var oMsgStart = {
		'type': 'I',
		'number': 1001,
		'text': 'Start writing of CSV'
	};
	var oMsgSuccess = {
		'type': 'S',
		'number': 1002,
		'text': 'Writing of CSV finished successfully. LoadId = &1, #Lines=&2'
	};
	var oMsgFailed = {
		'type': 'E',
		'number': 1003,
		'text': 'Writing of CSV failed'
	};

	function MsgErrorDetail(sText) {
		var oMe = this;
		oMe.type = 'E';
		oMe.number = 99999;
		oMe.text = sText;
	}

	var oMetadata = {};

	return async.waterfall(
		[
			cb1 => getMetadata.doIt(
				oTrace, oClient, sSchema, sNameDSO, cb1
			),
			(oMD, cb1) => {
				oMetadata = oMD;
				return cb1();
			},
			cb1 => getNewId.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				'OPERATION_REQUEST', sOperationLoad, sStatusRunning, false /*withcommit*/ , cb1
			),
			(opId, cb1) => {
				operationId = opId;
				return writeMessage.doIt(
					oTrace, oClient2Connection, sSchema, sNameDSO, oMetadata,
					operationId, [oMsgStart], true /*withCommit*/ , cb1
				);
			},
			cb1 => getNewId.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata, 'LOAD_REQUEST',
				null, null, false /*withcommit*/ , cb1
			),
			(lId, cb1) => {
				loadId = lId;
				return cb1(null);
			},
			cb1 => oClient.commit(cb1),

			cb1 => writeAffectedRequests.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				operationId, [loadId], false /*withcommit*/ , cb1
			),
			cb1 => updateOperationStatus.doIt(
				oTrace, oClient, sSchema, sNameDSO, oMetadata,
				operationId, sStatusFinished, false /*withCommit*/ , cb1
			),
			cb1 => {
				var noOfColumns = 0;
				var recordNo = 1;
				var sFieldlist = '';
				if (Array.isArray(data)) {
					noOfColumns = data[0].length + 2;
					if (withHeaderLine) {
						aFieldList = _.flatten(
							[
								sLoadIdFieldname,
								sRecordNoFieldname,
								_.map(data[0], fieldname => '"' + fieldname + '"')
							]);
						sFieldlist = aFieldList.join(',');
						data.shift();
					}
					aData = _.map(data, row => _.flatten([loadId, recordNo++, row]));
				} else {
					// convert 'flat' csv string into an array of arrays (lines with columns)
					var aLines = _.split(data, '\n');
					if (withHeaderLine) {
						aFieldList = _.flatten(
							[
								sLoadIdFieldname,
								sRecordNoFieldname,
								_.map(
									_.split(
										aLines[0],
										';'
									),
									fieldname => '"' + fieldname.trim() + '"'
								)
							]
						);
						sFieldlist = aFieldList.join(',');
						aLines.shift();
					}
					aData = aLines
						.filter(s => s !== '')
						.map(function(sLine) {
							var aColumns = _.flatten(
								[
									loadId,
									recordNo++,
									_.split(sLine, ';')
								]);
							noOfColumns = aColumns.length;
							return aColumns;
						});
				}
				if (!sFieldlist) {
					aFieldList = _.map(
						_.find(
							oMetadata.activationQueues,
							activationQueue => activationQueue.name === sActivationQueue
						).fields,
						field => [
							'"',
							hdbext.sqlInjectionUtils.escapeDoubleQuotes(field.name),
							'"'
						].join('')
					);
					sFieldlist = aFieldList.join(',');
				}
				// create sql-statement with appropriate number of variables
				sSql = [
					'insert into "',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
					'"."',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
					'.',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sActivationQueue),
					'" ',
					' (',
					sFieldlist,
					' ) ',
					' values (',
					_.range(
						noOfColumns
					).map(_.constant('?'))
					.join(','),
					')'
				].join('');

				// execute mass insert
				return oClient.prepare(sSql, cb1);
			},
			(statement, cb1) => statement.exec(aData, cb1),
			(affectedRows, cb1) => cb1(null, affectedRows.length),
			(rC, cb1) => {
				rowCount = rC;
				oMsgSuccess.text = _.replace(_.replace(oMsgSuccess.text, '&1', loadId), '&2', rowCount);
				return writeMessage.doIt(
					oTrace, oClient, sSchema, sNameDSO, oMetadata,
					operationId, [oMsgSuccess], false /*withCommit*/ , cb1
				);
			},
			cb1 => oClient.commit(cb1)
		],

		err => err ? async.waterfall(
			[
				cb1 => {
					var aMessages = [];

					if (err.message) {
						aMessages.push(new MsgErrorDetail(err.message));
						aMessages.push(new MsgErrorDetail(sSql));
						/*
						var r = /^.*line ([0-9]+) col ([0-9]+) .*$/;
						var a = r.exec( err.message );
						if( a && a.length === 3 ) {
						    aMessages.concat(
						        aFieldList.map(
						            ( s, i ) => new MsgErrorDetail(
						                [
						                    'Column ',
						                    i,
						                    ' ',
						                    s,
						                    ': ',
						                    aData[parseInt( a[1] )][ parseInt( a[2] )]
						                ].join( '' )
						            )
						        )
						    );
						}
						*/
					}
					if (err.stack) {
						aMessages.push(new MsgErrorDetail(err.stack));
					}
					aMessages.push(oMsgFailed);
					return writeMessage.doIt(
						oTrace, oClient2Connection, sSchema, sNameDSO, oMetadata,
						operationId, aMessages, true /*withCommit*/ , cb1
					);
				},
				cb1 => oClient.rollback(cb1),
				cb1 => updateOperationStatus.doIt(
					oTrace, oClient2Connection, sSchema, sNameDSO, oMetadata,
					operationId, sStatusFailed, true /*withCommit*/ , cb1
				)
			],
			err1 => {
				if (err1) {
					oTrace.error('error in rollback of "storeCSV"');
					oTrace.error(err1);
					return cb(err1);
				}
				// do not send error to caller
				return cb(null, {
					'operationId': operationId,
					'loadId': loadId
				});
			}
		) : cb(null, {
			'operationId': operationId,
			'loadId': loadId
		})
	);
};