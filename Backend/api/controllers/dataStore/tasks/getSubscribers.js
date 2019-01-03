/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
var _ = require('lodash');
var hdbext = require('@sap/hdbext');
var getMetadata = require('./getMetadata');

exports.doIt = function(oTrace, oClient, sSchema, sNameDSO, cb) {

	oTrace.info('call of "getSubscribers( ' + sSchema + ', ' + sNameDSO + ' )"');

	if (!sSchema) {
		cb(new Error('No Schema provided'));
		return;
	}
	if (!sNameDSO) {
		cb(new Error('No DataStore provided'));
		return;
	}

	var subscriberNameColName = '"' + 'technicalKey.subscriberName' + '"';
	var descriptionColName = '"' + 'technicalAttributes.description' + '"';
	var userNameColName = '"' + 'technicalAttributes.userName' + '"';
	var creationTimestampColName = '"' + 'technicalAttributes.creationTimestamp' + '"';
	var maxRequestColName = '"' + 'technicalAttributes.maxRequest' + '"';
	var pushNotificationColName = '"' + 'technicalAttributes.pushNotification' + '"';

	async.waterfall(
		[
			cb1 => getMetadata.doIt(oTrace, oClient, sSchema, sNameDSO, cb1),
			(oMetadata, cb1) => {
				var sSubscriberTabName = [
					'"',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sSchema),
					'"."',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(sNameDSO),
					'.',
					hdbext.sqlInjectionUtils.escapeDoubleQuotes(oMetadata.subscribers.name),
					'"'
				].join('');
				var sSql =
					'select ' + subscriberNameColName + ', ' + descriptionColName + ', ' + userNameColName + ', ' + 'TO_NVARCHAR( ' +
					creationTimestampColName + ', \'YYYYMMDDHH24MISS\' )' + ' as ' + creationTimestampColName + ', ' + maxRequestColName + ', ' +
					pushNotificationColName + '  from ' + sSubscriberTabName;

				return oClient.prepare(sSql, cb1);
			},
			(statement, cb1) => statement.exec([], cb1),
			(aRows, cb1) => cb1(
				null,
				_.map(
					aRows,
					function(oRow) {
						var oSubscribers = {};
						_.each(
							oRow,
							function(oV, sK) {
								var aName = _.split(sK, '.');
								if (aName[aName.length - 1] === 'description') {
									oSubscribers[aName[aName.length - 1]] = oV.toString();
								} else {
									oSubscribers[aName[aName.length - 1]] = oV;
								}
							});
						return oSubscribers;
					}
				)
			),
			(aSubscribers, cb1) => cb1(null, {
				result: aSubscribers
			})
		],
		cb
	);
};