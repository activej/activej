'use strict';

var UIKernel = require('uikernel');
var validator = require('./validator');
var xhr = require('xhr');

var API_URL = '/api/users';

var users = new UIKernel.Models.Grid.Xhr({
  api: API_URL,
  validator: validator
});

users.delete = function (recordId, cb) {
  xhr({
    method: 'DELETE',
    headers: {'Content-type': 'application/json'},
    uri: API_URL + '/' + recordId
  }, function (err) {
    cb(err);
  });
};

module.exports = users;
