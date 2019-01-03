/* Copyright (c) 2016 SAP SE or an SAP affiliate company. All rights reserved. */
'use strict';

var Logger = require('./logger');
exports.logger = new Logger.doIt();

// metadata-methods
var getDataStores = require('./tasks/getDataStores');
var getDatastoreFeature = require('./tasks/getDatastoreFeature');
var getLogForOperation = require('./tasks/getLogForOperation');
var getMetadata = require('./tasks/getMetadata');
var getNewId = require('./tasks/getNewId');
var getOperationInfo = require('./tasks/getOperationInfo');
var getOperationsForRequest = require('./tasks/getOperationsForRequest');
var getRequestInfo = require('./tasks/getRequestInfo');
var getRequestsForActivation = require('./tasks/getRequestsForActivation');
var getRequestsForCleanup = require('./tasks/getRequestsForCleanup');
var getRequestsForDeletion = require('./tasks/getRequestsForDeletion');
var getRequestsForRollback = require('./tasks/getRequestsForRollback');
var getRowcountWithFilter = require('./tasks/getRowcountWithFilter');
var getSubscribers = require('./tasks/getSubscribers');

// tasks
var activate = require('./tasks/activate');
var addSubscriber = require('./tasks/addSubscriber');
var checkMetadataConsistency = require('./tasks/checkMetadataConsistency');
var cleanupMetadata = require('./tasks/cleanupMetadata');
var cleanupChangelog = require('./tasks/cleanupChangelog');
var deleteRequest = require('./tasks/deleteRequest');
var deleteWithFilter = require('./tasks/deleteWithFilter');
var deleteAll = require('./tasks/deleteAll');
var rollback = require('./tasks/rollback');
var storeCSV = require('./tasks/storeCSV');
var storeSQL = require('./tasks/storeSQL');
var removeSubscriber = require('./tasks/removeSubscriber');
var resetSubscriber = require('./tasks/resetSubscriber');
var repairRunningOperations = require('./tasks/repairRunningOperations');

exports.getDataStores = getDataStores.doIt;
exports.getDatastoreFeature = getDatastoreFeature.doIt;
exports.getLogForOperation = getLogForOperation.doIt;
exports.getMetadata = getMetadata.doIt;
exports.getNewId = getNewId.doIt;
exports.getOperationsForRequest = getOperationsForRequest.doIt;
exports.getOperationInfo = getOperationInfo.doIt;
exports.getRequestInfo = getRequestInfo.doIt;
exports.getRequestsForActivation = getRequestsForActivation.doIt;
exports.getRequestsForCleanup = getRequestsForCleanup.doIt;
exports.getRequestsForDeletion = getRequestsForDeletion.doIt;
exports.getRequestsForRollback = getRequestsForRollback.doIt;
exports.getRowcountWithFilter = getRowcountWithFilter.doIt;
exports.getSubscribers = getSubscribers.doIt;

exports.activate = activate.doIt;
exports.addSubscriber = addSubscriber.doIt;
exports.checkMetadataConsistency = checkMetadataConsistency.doIt;
exports.cleanupMetadata = cleanupMetadata.doIt;
exports.cleanupChangelog = cleanupChangelog.doIt;
exports.deleteAll = deleteAll.doIt;
exports.deleteRequest = deleteRequest.doIt;
exports.deleteWithFilter = deleteWithFilter.doIt;
exports.rollback = rollback.doIt;
exports.storeCSV = storeCSV.doIt;
exports.storeSQL = storeSQL.doIt;
exports.removeSubscriber = removeSubscriber.doIt;
exports.resetSubscriber = resetSubscriber.doIt;
exports.repairRunningOperations = repairRunningOperations.doIt;