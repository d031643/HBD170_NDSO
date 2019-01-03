/* Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var async = require('async');
//var util = require( 'util' );
var _ = require('lodash');
var getMetadata = require('./getMetadata');
var getNewId = require('./getNewId');
var updateOperationStatus = require('./updateOperationStatus');
var setOperationDetails = require('./setOperationDetails');
var writeMessage = require('./writeMessage');

exports.doIt = function(oTrace, oClient, oClient2Connection, sSchema, sNameDSO,
	subscriberName, cb) {

	oTrace.info('call of "removeSubscriber( ' + subscriberName + ', ' + sSchema + ', ' + sNameDSO + ' )"');

	if (!sSchema) {
		cb(new Error('No Schema provided'));
		return;
	}
	if (!sNameDSO) {
		cb(new Error('No DataStore provided'));
		return;
	}
	if (!subscriberName) {
		cb(new Error('No subscriber-name provided'));
		return;
	}

	// use constants from annotations
	var sOperation = 'REMOVE_SUBSCRIBER';
	var sStatusRunning = 'RUNNING';
	var sStatusFinished = 'FINISHED';
	var sStatusFailed = 'FAILED';

	var oMsgStart = {
		'type': 'I',
		'number': 11001,
		'text': 'Start remove-subscriber'
	};
	var oMsgSuccess = {
		'type': 'S',
		'number': 11002,
		'text': 'Remove-subscriber finished successfully.'
	};
	var oMsgFailed = {
		'type': 'E',
		'number': 11003,
		'text': 'Remove-subscriber failed'
	};

	function MsgErrorDetail(sText) {
		var oMe = this;
		oMe.type = 'E';
		oMe.number = 99999;
		oMe.text = sText;
	}

	var subscriberNameColName = '"' + 'technicalKey.subscriberName' + '"';

	var oMetadata = {};

	async.waterfall(
		[
			function(cb1) {
				getMetadata.doIt(oTrace, oClient, sSchema, sNameDSO, function(err, oMD) {
					if (err) {
						cb1(err);
						return;
					}

					oMetadata = oMD;

					cb1();
					return;
				});
			},

			function(cb1) {
				getNewId.doIt(oTrace, oClient, sSchema, sNameDSO, oMetadata,
					'OPERATION_REQUEST', sOperation, sStatusRunning, false /*withCommit*/ , cb1);
			},

			function(operationId, cb1) {
				writeMessage.doIt(oTrace, oClient2Connection, sSchema, sNameDSO, oMetadata,
					operationId, [oMsgStart], true /*withCommit*/ ,
					function(err) {
						if (err) {
							cb1(err, operationId);
							return;
						}
						cb1(null, operationId);
						return;
					});
			},

			function(operationId, cb1) {
				oClient.commit(function(err) {
					if (err) {
						if (err.aNdsoErrorDetails) {
							err.aNdsoErrorDetails.push({
								'sqlStatment': 'commit'
							});
						} else {
							err.aNdsoErrorDetails = [{
								'sqlStatment': 'commit'
							}];
						}

						cb1(err, operationId);
						return;
					}
					oTrace.info('commit');
					cb1(null, operationId);
					return;
				});
			},

			function(operationId, cb1) {
				setOperationDetails.doIt(oTrace, oClient, sSchema, sNameDSO, oMetadata,
					operationId, {
						'subscriberName': subscriberName
					}, false /*withCommit*/ ,
					function(err) {
						if (err) {
							cb1(err, operationId);
							return;
						}
						cb1(null, operationId);
						return;
					});
			},

			function(operationId, cb1) {
				updateOperationStatus.doIt(oTrace, oClient, sSchema, sNameDSO, oMetadata,
					operationId, sStatusFinished, false /*withCommit*/ ,
					function(err) {
						if (err) {
							cb1(err, operationId);
							return;
						}
						cb1(null, operationId);
						return;
					});
			},

			function(operationId, cb1) {
				var sSubscribersTabname = '"' + sSchema + '"."' + sNameDSO + '.' + oMetadata.subscribers.name + '"';

				var sSql =
					'delete from ' + sSubscribersTabname +
					'  where ' + subscriberNameColName + ' = ? ';

				oClient.prepare(sSql, function(err, statement) {
					if (err) {
						if (err.aNdsoErrorDetails) {
							err.aNdsoErrorDetails.push({
								'sqlStatment': 'exec: ' + sSql
							});
						} else {
							err.aNdsoErrorDetails = [{
								'sqlStatment': 'exec: ' + sSql
							}];
						}

						cb1(err, operationId);
						return;
					}
					cb1(null, operationId, statement, sSql);
					return;
				});
			},

			function(operationId, statement, sSql, cb1) {
				statement.exec([subscriberName], function(err) {
					if (err) {
						if (err.aNdsoErrorDetails) {
							err.aNdsoErrorDetails.push({
								'sqlStatment': 'exec: ' + sSql
							});
						} else {
							err.aNdsoErrorDetails = [{
								'sqlStatment': 'exec: ' + sSql
							}];
						}

						cb1(err, operationId);
						return;
					}
					cb1(null, operationId);
					return;
				});
			},

			function(operationId, cb1) {
				writeMessage.doIt(oTrace, oClient, sSchema, sNameDSO, oMetadata,
					operationId, [oMsgSuccess], false /*withCommit*/ ,
					function(err) {
						if (err) {
							cb1(err, operationId);
							return;
						}
						cb1(null, operationId);
					});
			},

			function(operationId, cb1) {
				oClient.commit(function(err) {
					if (err) {
						if (err.aNdsoErrorDetails) {
							err.aNdsoErrorDetails.push({
								'sqlStatment': 'commit'
							});
						} else {
							err.aNdsoErrorDetails = [{
								'sqlStatment': 'commit'
							}];
						}

						cb1(err, operationId);
						return;
					}
					oTrace.info('commit');
					cb1(null, operationId);
					return;
				});
			}
		],

		function(err, operationId) {
			if (err) {
				async.waterfall(
					[
						function(cb1) {
							var aMessages = [];
							if (err.aNdsoErrorDetails && err.aNdsoErrorDetails.length !== 0) {
								aMessages = _.flatten(_.map(err.aNdsoErrorDetails, function(oDetail) {
									return _.map(oDetail, oV => new MsgErrorDetail(oV.toString()));
								}));
							}
							if (err.message) {
								aMessages.push(new MsgErrorDetail(err.message));
							}
							if (err.stack) {
								aMessages.push(new MsgErrorDetail(err.stack));
							}
							aMessages.push(oMsgFailed);

							writeMessage.doIt(oTrace, oClient2Connection, sSchema, sNameDSO, oMetadata,
								operationId, aMessages, true /*withCommit*/ ,
								function(err1) {
									if (err1) {
										cb1(err1);
										return;
									}
									cb1();
									return;
								});
						},

						function(cb1) {
							oClient.rollback(function(err1) {
								if (err1) {
									if (err.aNdsoErrorDetails) {
										err.aNdsoErrorDetails.push({
											'sqlStatment': 'rollback'
										});
									} else {
										err.aNdsoErrorDetails = [{
											'sqlStatment': 'rollback'
										}];
									}

									cb1(err1);
									return;
								}
								oTrace.info('rollback');
								cb1();
								return;
							});
						},

						function(cb1) {
							updateOperationStatus.doIt(oTrace, oClient2Connection, sSchema, sNameDSO, oMetadata,
								operationId, sStatusFailed, true /*withCommit*/ ,
								function(err1) {
									if (err1) {
										cb1(err1);
										return;
									}
									cb1(err);
									return;
								});
						}
					],
					function(err1) {
						if (err1) {
							oTrace.error('error in rollback of "remove subscriber"');
							oTrace.error(err1);
						}

						// do not send error to caller
						cb(null, {
							'operationId': operationId
						});
						return;
					});
			} else {
				cb(null, {
					'operationId': operationId
				});
				return;
			}
		}
	);
};