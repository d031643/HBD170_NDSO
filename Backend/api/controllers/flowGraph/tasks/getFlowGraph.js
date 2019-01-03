/* Copyright (c) 2016 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');

exports.doIt = function(oTrace, oClient, sSchema, sFlowGraphName, cb) {
	if (!sSchema) {
		cb(new Error('No Schema provided'));
		return;
	}
	var oFlowGraph = null;
	async.waterfall(
		[
			cb1 => oClient.prepare(
				'select ' + [
					'TASK_NAME',
					'OWNER_NAME',
					'CREATE_TIME',
					'COMMENTS',
					'PROCEDURE_SCHEMA',
					'PROCEDURE_NAME'
				].join(',') +
				' from sys.tasks where schema_name = ?  and TASK_NAME = ?',
				cb1
			),
			(statement, cb1) => statement.exec([sSchema, sFlowGraphName], cb1),
			(aRows, cb1) => aRows.length !== 1 ? cb1(new Error('Flowgraph not found')) : cb1(null, aRows[0]),
			(o, cb1) => {
				oFlowGraph = o;
				return oClient.prepare(
					'select ' + [
						'parameter_name',
						'parameter_type',
						'table_type_name',
						'table_type_schema'
					].join(',') +
					' from sys.task_parameters where schema_name = ?  and TASK_NAME = ? order by position',
					cb1
				);
			},
			(statement, cb1) => statement.exec([sSchema, sFlowGraphName], cb1),
			(aRows, cb1) => {
				oFlowGraph.parameter = aRows;
				return cb1(null, oFlowGraph);
			}
		],
		cb
	);
};