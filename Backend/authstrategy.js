'use strict';

/*
 * A "hybrid" authentication strategy that tries validating authentication
 * of type enduser token first, then falls back to client credential validation.
 */

const xssec = require('@sap/xssec');

function JWTHybridStrategy(options) {
	this.options = options;
	this.name = 'JWT';
}

JWTHybridStrategy.prototype.authenticate = function(req) {
	let self = this;
	let authorization = req.headers.authorization;
	if (!authorization) {
		console.log('no auth header');
		return this.fail(401);
	}

	let parts = authorization.split(' ');
	if (parts.length < 2) {
		console.log('split error');
		return this.fail(400);
	}

	let scheme = parts[0];
	let token = parts[1];

	if (scheme.toLowerCase() !== 'bearer') {
		console.log('no bearer');
		return this.fail(401);
	}

	let clientCredentialsFallback = function(token, options) {
		xssec.createSecurityContextCC(token, options, function(err, ctx) {
			if (err) {
				console.log('sec context CC error');
				if (err.statuscode) {
					return self.fail(err.statuscode);
				} else {
					// something went wrong during validation
					console.log(err);
					console.log('something went wrong during validation');
					return self.error(err);
				}
			}
			let user = {
				id: 'TECHNICAL_USER',
				name: {
					givenName: 'TECHNICAL',
					familyName: 'USER'
				},
				emails: 'no-reply@mycompany.com'
			};
			// passport will set these in req.user & req.authInfo respectively
			console.log('Security Context created from access token');
			let scope = 'TOE';
			if (ctx.checkLocalScope(scope)) {
				console.log('Scope verified, sending response');
				self.success(user, ctx);
			} else {
				console.log(scope);
				self.error(new Error('scope check failed'));
			}
		});
	};

	try {
		// Try validating using enduser token first
		// except for instance operations, only client credentials are allowed
		if (req.path.startsWith('/instance')) {
			return clientCredentialsFallback(token, self.options);
		}
		xssec.createSecurityContext(token, this.options, function(err, ctx) {
			if (err) {
				console.log('sec context error');
				if (err.statuscode) {
					// If this did not work, check if it is a valid client credential.
					return clientCredentialsFallback(token, self.options);
				} else {
					// something went wrong during validation
					return self.error(err);
				}
			}
			let userInfo = ctx.getUserInfo();
			let user = {
				id: userInfo.logonName,
				name: {
					givenName: userInfo.firstName,
					familyName: userInfo.lastName
				},
				emails: [{
					value: userInfo.email
				}]
			};
			// passport will set these in req.user & req.authInfo respectively
			let scope = 'cloud_controller.read';
			if (ctx.checkScope(scope)) {
				self.success(user, ctx);
			} else {
				console.log(scope);
				self.error(new Error('scope check fails'));
			}
		});
	} catch (err) {
		// there was a general verification error
		console.log('general verification error');
		this.error(err);
	}

};

exports.JWTHybridStrategy = JWTHybridStrategy;