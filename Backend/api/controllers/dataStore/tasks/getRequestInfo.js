/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var _ = require('lodash');
var hdbext = require('@sap/hdbext');

var getMetadata = require('./getMetadata');

exports.doIt = function(oTrace, oClient, sSchema, sNameDSO,
	oOperationFilter, oStatusFilter, oTimeFilter, oRequestFilter, oUserFilter, cb) {

	oTrace.info('call of "getRequestInfo( ' + sSchema + ', ' + sNameDSO + ')"');

	if (!sSchema) {
		cb(new Error('No Schema provided'));
		return;
	}
	if (!sNameDSO) {
		cb(new Error('No DataStore provided'));
		return;
	}

	var idColName = '"' + 'technicalKey.id' + '"';
	var typeColName = '"' + 'technicalAttributes.type' + '"';
	var requestIdColName = '"' + 'technicalKey.requestId' + '"';
	var operationIdColName = '"' + 'technicalKey.operationId' + '"';
	var operationColName = '"' + 'technicalAttributes.operation' + '"';
	var userNameColName = '"' + 'technicalAttributes.userName' + '"';
	var startTimeColName = '"' + 'technicalAttributes.startTimestamp' + '"';
	var lastTimeColName = '"' + 'technicalAttributes.lastTimestamp' + '"';
	var statusColName = '"' + 'technicalAttributes.status' + '"';
	var operationDetailsColName = '"' + 'technicalAttributes.operationDetails' + '"';
	var checksumATColName = '"' + 'technicalAttributes.checksumAT' + '"';
	var checksumCLColName = '"' + 'technicalAttributes.checksumCL' + '"';

	var sAffectedRequestsTabName = '';
	var sIdGeneratorTabName = '';
	var sOperationHistoryTabName = '';

	async.waterfall(
		[
			cb1 => getMetadata.doIt(oTrace, oClient, sSchema, sNameDSO, cb1),
			(oMetadata, cb1) => {
				sAffectedRequestsTabName = [
					'"',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
					'"."',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
					'.',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(oMetadata.affectedRequests.name),
					'"'
				].join('');
				sIdGeneratorTabName = [
					'"',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
					'"."',
					sNameDSO,
					'.',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(oMetadata.idGenerator.name),
					'"'
				].join('');
				sOperationHistoryTabName = [
					'"',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
					'"."',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
					'.',
					oMetadata.operationHistory.name,
					'"'
				].join('');

				var aFilterCondition = [];

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

				if (oRequestFilter != null) {
					if (oRequestFilter.requests != null && oRequestFilter.requests.length > 0) {
						aFilterCondition.push(['T.' + requestIdColName + ' in (',
							oRequestFilter.requests.join(','),
							')'
						].join(' '));
					}
					if (oRequestFilter.low != null && oRequestFilter.high != null) {
						aFilterCondition.push('T.' + requestIdColName + ' between ' +
							oRequestFilter.low + ' and ' + oRequestFilter.high);
					} else if (oRequestFilter.low != null) {
						aFilterCondition.push('T.' + requestIdColName + ' >= ' + oRequestFilter.low);
					} else if (oRequestFilter.high != null) {
						aFilterCondition.push('T.' + requestIdColName + ' <= ' + oRequestFilter.high);
					}
				}

				if (oTimeFilter != null) {
					var s = 'TO_NVARCHAR( OP_HIST.' + lastTimeColName + ', \'YYYYMMDDHH24MISS\' ) ';
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

				var sSql =
					'select ' +
					'  T.' + requestIdColName + ', ' +
					'  T.' + typeColName + ', ' +
					'  OP_HIST.' + operationIdColName + ', ' +
					'  OP_HIST.' + operationColName + ', ' +
					'  OP_HIST.' + userNameColName + ', ' +
					'  TO_NVARCHAR( OP_HIST.' + startTimeColName + ', \'YYYYMMDDHH24MISS\' ) ' +
					'    as ' + startTimeColName + ', ' +
					'  TO_NVARCHAR( OP_HIST.' + lastTimeColName + ', \'YYYYMMDDHH24MISS\' ) ' +
					'    as ' + lastTimeColName + ', ' +
					'  OP_HIST.' + statusColName + ', ' +
					'  OP_HIST.' + operationDetailsColName + ', ' +
					'  OP_HIST.' + checksumATColName + ', ' +
					'  OP_HIST.' + checksumCLColName + ' ' +
					'from (select AFF_REQ.' + requestIdColName + ', ' +
					'        ID_GEN.' + typeColName + ', ' +
					'        max( AFF_REQ.' + operationIdColName + ' ) as ' + operationIdColName +
					'        from ' + sAffectedRequestsTabName + ' AFF_REQ ' +
					'        join ' + sIdGeneratorTabName + ' ID_GEN ' +
					'          on ID_GEN.' + idColName + ' = AFF_REQ.' + requestIdColName +
					'        where ID_GEN.' + typeColName + ' in ( \'LOAD_REQUEST\', \'ACTIVATION_REQUEST\' ) ' +
					'        group by AFF_REQ.' + requestIdColName + ', ' +
					'                 ID_GEN.' + typeColName + ' ) T ' +
					'join ' + sOperationHistoryTabName + ' OP_HIST ' +
					'  on T.' + operationIdColName + ' = ' + 'OP_HIST.' + operationIdColName +
					(aFilterCondition.length === 0 ? '' : '  where ' + aFilterCondition.join(' and '));
				return oClient.prepare(sSql, cb1);
			},
			(statement, cb1) => statement.exec([], cb1),
			(aRows, cb1) => async.mapSeries(
				aRows,
				(oRow, cb2) => {
					var oRequest = null;
					async.waterfall(
						[
							cb3 => {
								oRequest = {};
								_.each(oRow, function(oV, sK) {
									var aName = _.split(sK, '.');
									if (aName[aName.length - 1] === 'operationDetails') {
										oRequest[aName[aName.length - 1]] = (!oV || oV.length === 0 ? {} : JSON.parse(oV.toString()));
									} else {
										oRequest[aName[aName.length - 1]] = oV;
									}
								});

								// Get Request status (last operation, which has been finished successfully)
								var sSql =
									'select ' + operationColName + ' as "operation" ' +
									' from ' + sOperationHistoryTabName + ' OP_HIST ' +
									' where OP_HIST.' + operationIdColName + ' = ( ' +
									'   select max( OP_HIST1.' + operationIdColName + ' ) ' +
									'    from ' + sOperationHistoryTabName + ' OP_HIST1 ' +
									'    join ' + sAffectedRequestsTabName + ' AFF_REQ1 ' +
									'      on OP_HIST1.' + operationIdColName + ' = AFF_REQ1.' + operationIdColName +
									'    where OP_HIST1.' + statusColName + ' = \'FINISHED\'' +
									'      and AFF_REQ1.' + requestIdColName + ' = ' + oRequest.requestId + ' ) ';

								return oClient.prepare(sSql, cb3);
							},
							(statement, cb3) => statement.exec([], cb3),
							(aRows2, cb3) => {
								if (aRows2.length === 0) {
									oRequest.requestStatus = 'CREATED';
								} else {
									switch (aRows2[0].operation) {
										case 'LOAD':
											oRequest.requestStatus = 'LOADED';
											break;
										case 'DELETE_REQUEST':
											oRequest.requestStatus = 'DELETED';
											break;
										case 'ACTIVATE':
										case 'CLEANUP_CHANGELOG':
											oRequest.requestStatus = 'ACTIVATED';
											break;
										case 'ROLLBACK':
											oRequest.requestStatus = 'ROLLED-BACK';
											break;
										default:
											oRequest.requestStatus = 'UNKNOWN';
											break;
									}
								}
								return cb3(null);
							},
							function(cb3) {
								var sSql = '';
								if (oRequest.type === 'LOAD_REQUEST') {
									// read corresponding activation request (if existing)
									sSql =
										'select ID_GEN.' + idColName + ' as "activationId"' +
										' from ' + sIdGeneratorTabName + ' ID_GEN ' +
										' join ' + sAffectedRequestsTabName + ' AFF_REQ1 ' +
										'   on ID_GEN.' + idColName + ' = AFF_REQ1.' + requestIdColName +
										' where ID_GEN.' + typeColName + ' = \'ACTIVATION_REQUEST\'' +
										'   and AFF_REQ1.' + operationIdColName + ' = ( ' +
										'       select max( OP_HIST.' + operationIdColName + ' ) ' +
										'         from ' + sAffectedRequestsTabName + ' AFF_REQ2 ' +
										'         join ' + sOperationHistoryTabName + ' OP_HIST ' +
										'           on AFF_REQ2.' + operationIdColName +
										'            = OP_HIST.' + operationIdColName +
										'         where OP_HIST.' + operationColName + ' = \'ACTIVATE\'' +
										'           and AFF_REQ2.' + requestIdColName +
										'             = ' + oRequest.requestId + ' ) ';
								} else if (oRequest.type === 'ACTIVATION_REQUEST') {
									// read corresponding load requests
									sSql =
										'select ID_GEN.' + idColName + ' as "loadId"' +
										' from ' + sIdGeneratorTabName + ' ID_GEN ' +
										' join ' + sAffectedRequestsTabName + ' AFF_REQ1 ' +
										'   on ID_GEN.' + idColName + ' = AFF_REQ1.' + requestIdColName +
										' where ID_GEN.' + typeColName + ' = \'LOAD_REQUEST\'' +
										'   and AFF_REQ1.' + operationIdColName + ' = ( ' +
										'       select max( OP_HIST.' + operationIdColName + ' ) ' +
										'         from ' + sAffectedRequestsTabName + ' AFF_REQ2 ' +
										'         join ' + sOperationHistoryTabName + ' OP_HIST ' +
										'           on AFF_REQ2.' + operationIdColName +
										'            = OP_HIST.' + operationIdColName +
										'         where OP_HIST.' + operationColName + ' = \'ACTIVATE\'' +
										'           and AFF_REQ2.' + requestIdColName +
										'             = ' + oRequest.requestId + ' ) ';
								} else {
									return cb3(new Error('Invalid request'));
								}
								return oClient.prepare(sSql, cb3);
							},
							(statement, cb3) => statement.exec([], cb3),
							(aRows2, cb3) => {
								if (oRequest.type === 'LOAD_REQUEST') {
									oRequest.dependantRequests = aRows2.map(
										row => {
											return {
												'requestId': row['loadId']
											};
										});
								} else {
									oRequest.dependantRequests = (aRows2[0] ? [{
										'requestId': aRows2[0].activationId
									}] : []);
								}
								return cb3(null);
							}
						],
						err => cb2(err, oRequest)
					);
				},
				cb1
			),
			(aRequests, cb1) => {
				var a = _.groupBy(aRequests, function(request) {
					return request.type;
				});
				return cb1(null, {
					result: {
						loadRequests: a.LOAD_REQUEST,
						activationRequests: a.ACTIVATION_REQUEST
					}
				});
			}
		],
		cb
	);
};