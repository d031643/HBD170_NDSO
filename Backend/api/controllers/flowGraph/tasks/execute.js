/* Copyright (c) 2016 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var hdbext = require('@sap/hdbext');
var helpers = require('../../../helpers');
exports.doIt = function(oTrace, oClient, sSchema, sFlowGraphName, cb) {
	var o = null;
	oTrace.info('call of "getFlowGraphs( ' + sSchema + ' )"');
	if (!sSchema) {
		cb(new Error('No Schema provided'));
		return;
	}
	async.waterfall(
		[
			cb1 => oClient.prepare(
				'select ' + [
					'PROCEDURE_SCHEMA',
					'PROCEDURE_NAME'
				].join(',') +
				' from sys.tasks where schema_name = ? and TASK_NAME = ?',
				cb1
			),

			(statement, cb1) => statement.exec([sSchema, sFlowGraphName], cb1),
			(aRows, cb1) => cb1(aRows.length !== 1 ? new Error('Flowgraph not found') : null, aRows[0]),
			(o1, cb1) => {
				o = o1;
				return oClient.prepare(
					'select ' + ['SCHEMA_NAME', 'PROCEDURE_NAME'].join(',') +
					' from sys.procedures where schema_name = ? and procedure_name = ?',
					cb1
				);
			},
			(statement, cb1) => statement.exec(
				[
					(o.PROCEDURE_SCHEMA ? o.PROCEDURE_SCHEMA : sSchema),
					(o.PROCEDURE_NAME ? o.PROCEDURE_NAME : sFlowGraphName + '_SP')
				],
				cb1
			),
			(aRows, cb1) => cb1(
				aRows.length !== 1 ? new Error('Procedure of flowGraph not found') : null,
				aRows[0]
			),
			(o1, cb1) => oClient.prepare(
				[
					'CALL "',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(o1.SCHEMA_NAME),
					'"."',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(o1.PROCEDURE_NAME) + '"()'
				].join(''),
				cb1
			),
			(stm, cb1) => stm.exec({}, cb1)
		],
		err => cb(
			err ? new helpers.Messages([{
				severity: 'error',
				msg: 'Execution failed'
			}]).addError(err) : null, {
				severity: 'info',
				msg: 'sucessfully executed'
			}
		)
	);
};