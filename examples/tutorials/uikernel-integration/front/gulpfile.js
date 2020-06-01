'use strict';

var gulp = require('gulp');
var sync = require('gulp-sync')(gulp).sync;
var shell = require('gulp-shell');

var jsTasks = require('./webapp/gulp/javascript');

// Javascript tasks
gulp.task('js:clean', gulp.series(jsTasks.jsClean));
gulp.task('js:style', gulp.series(jsTasks.copyLess));
gulp.task('js:bundle', gulp.series(jsTasks.createBundle));

// Java tasks
gulp.task('java:clean', gulp.series(shell.task(['mvn -f ../pom.xml clean'])));
gulp.task('java:test', gulp.series(shell.task(['mvn -f ../pom.xml test'])));
gulp.task('java:compile', gulp.series(shell.task(['mvn -f ../pom.xml compile'])));
gulp.task('java:package', gulp.series(shell.task(['mvn -f ../pom.xml package'])));
gulp.task('java:run', gulp.series(shell.task(['java -jar ../target/uikernel-integration.jar'])));


// General tasks
gulp.task('clean', gulp.parallel('js:clean', 'java:clean'));
gulp.task('test', gulp.parallel('java:test'));
gulp.task('build', gulp.series((['clean', 'js:bundle', 'js:style', 'java:compile'])));
gulp.task('pack', gulp.series((['js:bundle', 'java:package'])));
gulp.task('run', gulp.parallel('java:run'));

gulp.task('default', gulp.series((['build', 'pack', 'run'])));
