'use strict';
var router = require('express').Router();
var TaskChain = require('./api/controllers/TaskChain');

router.get('/taskType/ndso/dataStoreName/inputHelp', TaskChain.getDataStores4VH);
router.get('/taskType/flowGraph/flowGraphName/inputHelp', TaskChain.getFlowGraphVH);
router.get('/taskType/nDso/fileName/inputHelp', TaskChain.getFiles);

router.get('/taskType/ndso/v1', TaskChain.getTaskTypesNdso);
router.get('/taskType/flowGraph/v1', TaskChain.getTaskTypesFlowGraph);

router.post('/taskType/ndso/v1/activate', TaskChain.activateRequests);
router.post('/taskType/flowGraph/v1/execute', TaskChain.executeFlowGraph);
router.post('/taskType/ndso/v1/loadFile', TaskChain.loadFile);
router.post('/taskType/ndso/v1/loadHTTP', TaskChain.loadHTTP);
router.post('/taskType/ndso/v1/loadSQL', TaskChain.loadSQL);

module.exports = router;