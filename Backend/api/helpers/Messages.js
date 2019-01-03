/* Copyright (c) 2016 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

function Messages(aMsg) {
	var oMe = this;
	var a = [];
	if (aMsg) {
		a = a.concat(aMsg);
	}
	oMe.getMessages = function() {
		return a;
	};
	oMe.addError = function(err) {
		a = a.concat(
			(err instanceof(Error)) ? [{
				severity: 'error',
				msg: err.message ? err.message.toString() : 'No message',
				timestamp: (new Date()).toString()
			}, {
				severity: 'error',
				msg: err.stack ? err.stack.toString() : 'No stack',
				timestamp: (new Date()).toString()
			}] : [{
				severity: 'error',
				msg: 'Unspecified internal error',
				timestamp: (new Date()).toString()
			}]
		);
		return oMe;
	};
}
exports.Messages = Messages;