/* Copyright (c) 2016 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';
var async = require('async');
var helpers = require('../helpers');
var fgTasks = require('./flowGraph/flowGraphTasks');
var trace = helpers.trace;

function ErrorResponse(oError) {
	this.message = oError.message;
	this.stack = oError.stack;
}

function handleResponse(res, err, o) {
	if (err) {
		res.status(500);
		res.send(new ErrorResponse(err));
	} else {
		res.send(o);
	}
}
module.exports = {
	getFlowGraphs: function(req, res) {
		var oCP = null;
		async.waterfall(
			[
				helpers.dbClient.createDBClientPair,
				(oClientPair, cb1) => {
					oCP = oClientPair;
					cb1(null);
				},
				cb1 => fgTasks.getFlowGraphs(
					trace,
					oCP.client1,
					oCP.schema,
					cb1
				),
				(a, cb1) => cb1(null, {
					list: a.map(
						fg => {
							var a1 = fg.match('^.*\\.(.*)$');
							var s = a1 ? a1[1] : fg;
							return {
								name: fg,
								text: s
							};
						}
					)
				})
			],
			(err, o) => {
				helpers.dbClient.closeDBClientPair(oCP);
				handleResponse(res, err, o);
			}
		);
	},
	getFlowGraph: function(req, res) {
		var oCP = null;
		async.waterfall(
			[
				helpers.dbClient.createDPClientPair,
				(oClientPair, cb1) => {
					oCP = oClientPair;
					return cb1(null);
				},
				cb1 => fgTasks.getFlowGraph(
					trace,
					oCP.client1,
					oCP.schema,
					req.swagger.params.flowGraphName.value,
					cb1
				)
			],
			(err, o) => {
				helpers.dbClient.closeDBClientPair(oCP);
				handleResponse(res, err, o);
			}
		);
	},
	execute: function(req, res) {
		var oCP = null;
		async.waterfall(
			[
				helpers.dbClient.createDBClientPair,
				(oClientPair, cb1) => {
					oCP = oClientPair;
					cb1(null);
				},
				cb1 => fgTasks.execute(
					trace,
					oCP,
					oCP.schema,
					req.swagger.params.flowGraphName.value,
					cb1
				)
			],
			(err, o) => {
				helpers.dbClient.closeClientPair(oCP);
				handleResponse(res, err, o);
			});
	}
};