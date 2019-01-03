/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');

exports.doIt = function(oTrace, oClient, sSchema, sNameDSO, cb) {

	oTrace.info('call of "getMetadata( ' + sSchema + ', ' + sNameDSO + ' )"');

	if (!sSchema) {
		cb(new Error('No Schema provided'));
		return;
	}
	if (!sNameDSO) {
		cb(new Error('No DataStore provided'));
		return;
	}

	var o = {};

	async.waterfall(
		[
			cb1 => oClient.prepare(
				'select' +
				'  d.artifact_name, ' +
				'  d.element_name, ' +
				'  d.artifact_kind, ' +
				'  d.parent_artifact_name, ' +
				'  d.parent_element_name, ' +
				'  d.ordinal_number, ' +
				'  d.sql_data_type_name, ' +
				'  d.type_param_1, ' +
				'  d.type_param_2, ' +
				'  d.is_key, ' +
				'  v.value ' +
				'from ' +
				'  CDS_ARTIFACT_DEFINITION( ?, ?) as d ' +
				'  left outer join CDS_ANNOTATION_VALUES v ' +
				'    on  d.schema_name   = v.schema_name ' +
				'    and d.artifact_name = v.artifact_name ' +
				'    and d.element_name  = v.element_name ',
				cb1),
			(statement, cb1) => statement.exec([sSchema, sNameDSO], cb1),

			(aRows, cb1) => {
				// get annotations of context
				var aContexts = aRows.filter(row => row.ARTIFACT_KIND === 'CONTEXT');

				if (aContexts.length !== 1) {
					return cb1(new Error('Could not determine Context'));
				}

				var oContextValue = JSON.parse(aContexts[0].VALUE.toString()).value;

				o.name = aContexts[0].ARTIFACT_NAME;

				o.snapshotSupport = (
					oContextValue.snapshotSupport &&
					oContextValue.snapshotSupport !== 'false' ? true : false
				);

				o.computeChecksum = (
					oContextValue.computeChecksum &&
					oContextValue.computeChecksum !== 'false' ? true : false
				);

				o.operationHistory = {
					'name': oContextValue.entity.operationHistory
				};
				o.affectedRequests = {
					'name': oContextValue.entity.affectedRequests
				};
				o.aggregationHistory = {
					'name': oContextValue.entity.aggregationHistory
				};
				o.logMessages = {
					'name': oContextValue.entity.logMessages
				};
				o.idGenerator = {
					'name': oContextValue.entity.idGenerator
				};
				o.subscribers = {
					'name': oContextValue.entity.subscribers
				};
				o.runningOperations = {
					'name': oContextValue.entity.runningOperations
				};

				o.activeData = {
					'name': oContextValue.entity.activeData
				};
				// Get all 'base'-elements which belong to ActiveData-table
				var aElements = aRows.filter(function(row) {
					return row.ARTIFACT_KIND === 'ELEMENT' && row.ARTIFACT_NAME === sNameDSO + '.' + o.activeData.name && !aRows.some(function(row2) {
						return row2.PARENT_ELEMENT_NAME === row.ELEMENT_NAME;
					});
				});

				o.activeData.fields = aElements.map(function(element) {
					var oField = {};
					oField.name = element.ELEMENT_NAME;
					oField.isKey = (element.IS_KEY === 'TRUE' ? true : false);
					if (!element.VALUE) {
						// default aggregation is NOP
						oField.aggregation = 'NOP';
					} else {
						// read annotations of the element to get aggregation
						var oElementValue = JSON.parse(element.VALUE.toString()).value;
						oField.aggregation = oElementValue.aggregation;
					}
					return oField;
				});

				if (oContextValue.entity.changeLog && oContextValue.entity.changeLog.length > 0) {
					o.changeLog = {
						'name': oContextValue.entity.changeLog
					};
					// Get all 'base'-elements which belong to changelog
					aElements = aRows.filter(
						row => row.ARTIFACT_KIND === 'ELEMENT' && row.ARTIFACT_NAME === sNameDSO + '.' + o.changeLog.name && !aRows.some(row2 => row2.PARENT_ELEMENT_NAME ===
							row.ELEMENT_NAME)
					);

					o.changeLog.fields = aElements.map(
						element => {
							return {
								name: element.ELEMENT_NAME,
								isKey: (element.ELEMENT_NAME.search('technicalKey.') === -1 ? false : true)
							};
						}
					);
				}

				o.activationQueues = oContextValue.entity.activationQueue.map(
					activationQueue => {
						return {
							name: activationQueue,
							fields: aRows.filter(function(row) {
								return row.ARTIFACT_KIND === 'ELEMENT' && row.ARTIFACT_NAME === sNameDSO + '.' + activationQueue && !aRows.some(function(row2) {
									return row2.PARENT_ELEMENT_NAME === row.ELEMENT_NAME;
								});
							}).map(
								element => {
									return {
										name: element.ELEMENT_NAME,
										isKey: (element.ELEMENT_NAME.search(
											'technicalKey.'
										) === -1 ? false : true),
										sqlDataTypename: element.SQL_DATA_TYPE_NAME,
										typeParam1: element.TYPE_PARAM_1,
										typeParam2: element.TYPE_PARAM_2,
										aggregation: (!element.VALUE) ? 'MOV' : (function(oElem) {
											return (
												oElem &&
												oElem.value &&
												oElem.value.aggregation
											) ? oElem.value.aggregation : 'MOV';
										}(
											JSON.parse(
												element.VALUE.toString()
											)
										))
									};
								}
							)
						};
					}
				);
				return cb1(null, o);
			}
		],
		(err, o1) => cb(err, o1)
	);
};