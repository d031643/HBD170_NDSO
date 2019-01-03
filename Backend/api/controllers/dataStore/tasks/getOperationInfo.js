/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
//var util = require( 'util' );
var _ = require('lodash');
var getMetadata = require('./getMetadata');

exports.doIt = function(oTrace, oClient, sSchema, sNameDSO,
	oOperationFilter, oStatusFilter, oTimeFilter, oOperationIdFilter, oUserFilter, cb) {

	oTrace.info('call of "getOperationInfo( ' + oOperationFilter + ', ' + sSchema + ', ' + sNameDSO + ' )"');

	if (!sSchema) {
		cb(new Error('No Schema provided'));
		return;
	}
	if (!sNameDSO) {
		cb(new Error('No DataStore provided'));
		return;
	}

	var operationIdColName = '"' + 'technicalKey.operationId' + '"';
	var operationColName = '"' + 'technicalAttributes.operation' + '"';
	var userNameColName = '"' + 'technicalAttributes.userName' + '"';
	var lastTimestampColName = '"' + 'technicalAttributes.lastTimestamp' + '"';
	var statusColName = '"' + 'technicalAttributes.status' + '"';
	var operationDetailsColName = '"' + 'technicalAttributes.operationDetails' + '"';

	var sOperationHistoryTabName = '';
	var aFilterCondition = [];

	async.waterfall(
		[
			function(cb1) {
				if (oOperationFilter != null) {
					if (oOperationFilter.operations != null && oOperationFilter.operations.length > 0) {
						aFilterCondition.push(['OP_HIST.' + operationColName + ' in (',
							oOperationFilter.operations.map(function(element) {
								return '\'' + element + '\'';
							}).join(','),
							')'
						].join(' '));
					}
				}

				if (oStatusFilter != null) {
					if (oStatusFilter.status != null && oStatusFilter.status.length > 0) {
						aFilterCondition.push(['OP_HIST.' + statusColName + ' in (',
							oStatusFilter.status.map(function(element) {
								return '\'' + element + '\'';
							}).join(','),
							')'
						].join(' '));
					}
				}

				if (oUserFilter != null) {
					if (oUserFilter.users != null && oUserFilter.users.length > 0) {
						aFilterCondition.push(['OP_HIST.' + userNameColName + ' in (',
							oUserFilter.users.map(function(element) {
								return '\'' + element + '\'';
							}).join(','),
							')'
						].join(' '));
					}
				}

				if (oOperationIdFilter != null) {
					if (oOperationIdFilter.requests != null && oOperationIdFilter.requests.length > 0) {
						aFilterCondition.push(['OP_HIST.' + operationIdColName + ' in (',
							oOperationIdFilter.requests.join(','),
							')'
						].join(' '));
					}
					if (oOperationIdFilter.low != null && oOperationIdFilter.high != null) {
						aFilterCondition.push('OP_HIST.' + operationIdColName + ' between ' +
							oOperationIdFilter.low + ' and ' + oOperationIdFilter.high);
					} else if (oOperationIdFilter.low != null) {
						aFilterCondition.push('OP_HIST.' + operationIdColName + ' >= ' + oOperationIdFilter.low);
					} else if (oOperationIdFilter.high != null) {
						aFilterCondition.push('OP_HIST.' + operationIdColName + ' <= ' + oOperationIdFilter.high);
					}
				}

				if (oTimeFilter != null) {
					var s = 'TO_NVARCHAR( OP_HIST.' + lastTimestampColName + ', \'YYYYMMDDHH24MISS\' ) ';
					if (oTimeFilter.low != null && oTimeFilter.high != null) {
						aFilterCondition.push(s + ' between ' + oTimeFilter.low + ' and ' + oTimeFilter.high);
					} else if (oTimeFilter.low != null) {
						aFilterCondition.push(s + ' >= ' + oTimeFilter.low);
					} else if (oTimeFilter.high != null) {
						aFilterCondition.push(s + ' <= ' + oTimeFilter.high);
					} else {
						cb(new Error('invalid time filter'));
						return;
					}
				}

				cb1();
				return;
			},

			function(cb1) {
				getMetadata.doIt(oTrace, oClient, sSchema, sNameDSO, cb1);
			},

			function(oMetadata, cb1) {
				sOperationHistoryTabName
					= '"' + sSchema + '"."' + sNameDSO + '.' + oMetadata.operationHistory.name + '"';

				var sSql =
					'select ' +
					'  OP_HIST.' + operationIdColName + ', ' +
					'  OP_HIST.' + operationColName + ', ' +
					'  OP_HIST.' + statusColName + ', ' +
					'  OP_HIST.' + userNameColName + ', ' +
					'  TO_NVARCHAR( OP_HIST.' + lastTimestampColName + ', \'YYYYMMDDHH24MISS\' ) ' +
					'    as ' + lastTimestampColName + ', ' +
					'  OP_HIST.' + operationDetailsColName +
					' from ' + sOperationHistoryTabName + ' OP_HIST ' +
					(aFilterCondition && aFilterCondition.length > 0 ? 'where ' + aFilterCondition.join(' and ') : '') +
					' order by OP_HIST.' + lastTimestampColName;

				oClient.prepare(sSql, function(err, statement) {
					if (err) {
						if (err.aNdsoErrorDetails) {
							err.aNdsoErrorDetails.push({
								'sqlStatment': 'exec: ' + sSql
							});
						} else {
							err.aNdsoErrorDetails = [{
								'sqlStatment': 'exec: ' + sSql
							}];
						}

						cb1(err);
						return;
					}
					cb1(null, statement, sSql);
					return;
				});
			},

			function(statement, sSql, cb1) {
				statement.exec([], function(err, aRows) {
					if (err) {
						if (err.aNdsoErrorDetails) {
							err.aNdsoErrorDetails.push({
								'sqlStatment': 'exec: ' + sSql
							});
						} else {
							err.aNdsoErrorDetails = [{
								'sqlStatment': 'exec: ' + sSql
							}];
						}

						cb1(err);
						return;
					}

					var aOperationInfo = _.map(aRows, function(oRow) {
						var oOperationInfo = {};
						_.each(oRow, function(sV, sK) {
							var aName = _.split(sK, '.');
							if (aName[aName.length - 1] === 'operationDetails') {
								oOperationInfo[aName[aName.length - 1]] = (sV.length === 0 ? {} : JSON.parse(sV.toString()));
							} else {
								oOperationInfo[aName[aName.length - 1]] = sV;
							}
						});
						return oOperationInfo;
					});

					cb1(null, aOperationInfo);
					return;
				});
			}
		],

		function(err, aOperationInfo) {
			if (err) {
				cb(err);
				return;
			}
			cb(null, {
				result: aOperationInfo
			});
			return;
		});
};