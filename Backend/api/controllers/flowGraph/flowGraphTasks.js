/* Copyright (c) 2016 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var Logger = require('./logger');
var getFlowGraphs = require('./tasks/getFlowGraphs');
var getFlowGraph = require('./tasks/getFlowGraph');
var execute = require('./tasks/execute');

exports.logger = new Logger.doIt();

exports.getFlowGraphs = getFlowGraphs.doIt;
exports.getFlowGraph = getFlowGraph.doIt;
exports.execute = execute.doIt;