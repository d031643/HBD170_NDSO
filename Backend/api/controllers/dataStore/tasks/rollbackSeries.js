/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var _ = require('lodash');
var deleteAggregationHistory = require('./deleteAggregationHistory');

exports.doIt = function(oTrace, oClient, sSchema, sNameDSO,
	oMetadata, aActivationIds, aAggregationHistory, cb) {

	oTrace.info('call of "rollbackSeries( [' + (aActivationIds ? aActivationIds.join(',') : '') + '], ' + sSchema + ', ' + sNameDSO + ' )"');

	if (!sSchema) {
		return cb(new Error('No Schema provided'));
	}
	if (!sNameDSO) {
		return cb(new Error('No DataStore provided'));
	}

	var requestIdColName = '"' + 'technicalKey.requestId' + '"';
	var activationQueueNameColName = '"' + 'technicalKey.activationQueueName' + '"';
	var elementNameColName = '"' + 'technicalKey.elmentName' + '"';
	var aggregationColName = '"' + 'technicalAttributes.aggregation' + '"';

	return async.eachSeries(
		aActivationIds,
		(activationId, cb1) => {
			async.eachSeries(
				oMetadata.activationQueues,
				(oActivationQueue, cb2) => {
					var aAH = _.filter(
						aAggregationHistory,
						aggrHist => aggrHist[
							requestIdColName
						] === activationId.name && aggrHist[
							activationQueueNameColName
						] === oActivationQueue.name
					);
					return async.waterfall(
						[
							cb3 => {
								var sSql = 'CALL DSO_ROLLBACK_CHANGES(?, ?, ?, ?, ?, ?, ?, ?)';
								oClient.prepare(sSql, cb3);
							},
							(statement, cb3) => {
								var oParameter = {
									'odsName': sNameDSO,
									'updateOps': oActivationQueue.fields.map(
										field => {
											var aAggr = _.filter(
												aAH,
												aggrHist => aggrHist[
													elementNameColName
												] === field.name
											);
											var aggregationAQ = (
												aAggr.length === 0 ? 'MOV' :
												aAggr[0][aggregationColName]
											);
											var aFieldAT = _.filter(
												oMetadata.activeData.fields,
												field1 => field1.name === field.name
											);
											var aggregationAT = (
												aFieldAT.length === 0 ? 'NOP' : aFieldAT[
													0
												].aggregation);
											if (aggregationAQ === 'NOP' && aggregationAT === 'NOP') {
												return;
											}
											return {
												column: field.name,
												aggregationBehavior: aggregationAQ,
												negateBeforeImages: (
													aggregationAT === 'SUM' ? true : false
												)
											};
										})
								};
								statement.exec({
										'CHANGE_LOG_SCHEMA_NAME': sSchema,
										'CHANGE_LOG_TABLE_NAME': sNameDSO + '.' + oMetadata.changeLog.name,
										'TARGET_SCHEMA_NAME': sSchema,
										'TARGET_TABLE_NAME': sNameDSO + '.' + oMetadata.activeData.name,
										'USAGE_MODE': 'CDSO',
										'ACTIVATION_IDS': JSON.stringify([activationId].map(parseInt)),
										'PARAMETER': JSON.stringify(oParameter)
									},
									cb3
								);
							},
							(parameters, cb3) => cb3(null, JSON.parse(parameters['RESULT'].toString()))
						],
						cb2
					);
				},
				cb1
			);
		},
		(err, aResults) => err ? cb(err) : async.waterfall(
			[
				cb1 => deleteAggregationHistory.doIt(
					oTrace, oClient, sSchema, sNameDSO,
					oMetadata, aActivationIds, false /*withCommit*/ , cb1
				)
			],
			err1 => err1 ? cb(err1) : cb(null, _.flatten(aResults))
		)
	);
};