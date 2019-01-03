/* Copyright (c) 2016 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

function Logger() {
	var oMe = this;
	oMe.log = function() {
		console.log(Array.prototype.slice.call(arguments).join('/n'));
		return null;
	};
	oMe.debug = oMe.log;
	oMe.info = oMe.log;
	oMe.error = oMe.log;
}
exports.doIt = Logger;