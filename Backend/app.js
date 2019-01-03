'use strict';
var express = require('express');
var passport = require('passport');
var xsenv = require('@sap/xsenv');
var bodyParser = require('body-parser');
var helpers = require('./api/helpers');
var trace = helpers.trace;
var registerTaskGroups = require('./registerTaskGroups');
var strat = require('./authstrategy');
var router = require('./Router');

var PORT = process.env.PORT || 3000;
var app = express();
module.exports = app; // for testing

var config = {
	appRoot: __dirname // required config
};
//Register middleware
/*
if( process.env.PORT ) {
    passport.use(
        'JWT',
        new strat.JWTHybridStrategy(
            xsenv.getServices({ uaa: { tag: 'xsuaa' } }).uaa
        )
    );
    app.use( passport.initialize() );
    app.use(
        '/',
        passport.authenticate( 'JWT', { session: false })
    );
}
*/
//Publish  middleware
app.use(bodyParser.json());
app.use('/backend', router);

//Register Task Groups to the dws servise
app.use(
	(err1, req, resp, next) => {
		if (err1) {
			trace.error(err1);
		}
		return next(err1, resp);
	}
);
app.listen(
	PORT, err => {
		if (err) {
			trace.error(err);
			return process.exit(2);
		}
		registerTaskGroups.doIt(
			err1 => {
				if (err1) {
					trace.error(err1);
					return process.exit(1)
				}
				trace.log('Backend module listening on: ' + PORT);
			}
		);
	}
);