/* Copyright (c) 2016 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';
var _ = require('lodash');
var util = require('util');
var trace = _.constant(console)();

function objToString(o) {
	return (o && o.getMessages) ? [
		'List of Messages:',
		o.getMessages().map(msg => '\t' + msg.severity + '\t' + msg.msg).join('\n')
	].join(' \n') : [
		'Native object: ', util.inspect(
			o, {
				showhidden: false,
				depth: null
			}
		)
	].join('\n');
}

exports.log = trace.log;
exports.error = trace.error;
exports.info = trace.info;
exports.warn = trace.warn;
exports.logObj = function(o) {
	trace.log(objToString(o));
};
exports.objToString = objToString;