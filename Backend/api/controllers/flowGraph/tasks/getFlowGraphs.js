/* Copyright (c) 2016 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');

exports.doIt = function(oTrace, oClient, sSchema, cb) {
	return (!sSchema) ? cb(new Error('No Schema provided')) : async.waterfall(
		[
			cb1 => oClient.prepare(
				'select TASK_NAME from sys.tasks where schema_name = ? order by task_name',
				cb1
			),
			(statement, cb1) => statement.exec([sSchema], cb1),
			(aRows, cb1) => cb1(null, aRows.map(oRow => oRow['TASK_NAME']))
		],
		cb
	);
};