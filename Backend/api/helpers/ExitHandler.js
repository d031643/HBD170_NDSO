/* Copyright (c) 2016 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';
var async = require('async');
var ON_DEATH = require('death');
var a = [];

function OnExit() {
	var oMe = this;
	oMe.addHandler = f => {
		a.push(f);
		return a.length;
	};
	oMe.removeHandler = n => {
		a[n] = null;
		if (
			a.every(f => f)
		) {
			a.length = 0;
		}
	};
}
process.on(
	'uncaughtException',
	err => async.waterfall(
		[
			cb => {
				console.error('An unhandled exception occured');
				console.error(err);
				console.error(err.message);
				console.error(err.stack);
				console.error('Inform toe .... \n');
				return cb(null, a.filter(f => typeof(f) === 'function'));
			},
			(a1, cb) => async.mapSeries(
				a1,
				(f, cb1) => f(cb1),
				cb
			)
		],
		(err1, results) => {
			if (err1) {
				console.error(err);
			}
			if (results && results.length) {
				results.forEach(o => console.log(o));
			}
			console.log('\n ................... done! Good bye. ');
			process.exit(2);
		}
	)
);

ON_DEATH(
	(signal, err) => err ? console.log('On Death: ' + err.message) : console.log('Bye')
);

var oOnExit = new OnExit();
exports.addHandler = oOnExit.addHandler.bind(oOnExit);
exports.removeHandler = oOnExit.removeHandler.bind(oOnExit);