'use strict';

var UIKernel = require('uikernel');

var validator = UIKernel.createValidator()
  .field('name', UIKernel.Validators.regExp(/^\w{2,30}$/, 'Invalid first name.'))
  .field('phone', UIKernel.Validators.regExp(/^(\d{3}-)?\d{2,10}$/, 'Invalid phone number.'))
  .field('age', UIKernel.Validators.number(1, 100, 'Invalid age.'))
  .field('gender', UIKernel.Validators.enum(['MALE', 'FEMALE'], 'Invalid gender.'));

module.exports = validator;
