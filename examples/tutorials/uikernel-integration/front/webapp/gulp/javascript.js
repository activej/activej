'use strict';

var gulp = require('gulp');
var browserify = require('browserify');
var fs = require('fs');
var del = require('del');
var less = require('gulp-less');
var rename = require('gulp-rename');

var BUNDLE_PATH = '../src/main/resources/static/js/bundle.js';

function copyLess() {
  return gulp.src('../src/main/resources/static/bower_components/uikernel/themes/base/main.less')
    .pipe(less())
    .pipe(rename('main.css'))
    .pipe(gulp.dest('../src/main/resources/static/bower_components/uikernel/themes/base'));
}

function createBundle() {
  return browserify('./webapp/src')
    .transform('babelify', {presets: ['react']})
    .bundle()
    .pipe(fs.createWriteStream(BUNDLE_PATH));
}


function jsClean() {
  return del(BUNDLE_PATH, {force: true});
}

module.exports = {
  createBundle: createBundle,
  jsClean: jsClean,
  copyLess: copyLess
};
