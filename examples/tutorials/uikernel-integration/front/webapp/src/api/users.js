'use strict';

var UIKernel = require('uikernel');
var usersModel = require('../models/users');

function userApi(router) {
  // Pass a model instance to Express API
  UIKernel.gridExpressApi(router).model(usersModel);

  // Add our own method to Express API
  router.delete('/:recordId', function (req, res, next) {
    usersModel.delete(req.params.recordId, function (err) {
      if (err) {
        return next(err);
      }
      res.send('OK');
    });
  });
}

module.exports = userApi;
