'use strict';

var React = require('react');
var GridComponent = require('./Grid');
var Popups = require('../common/popup/Popups');
var createClass = require('create-react-class');

var AppComponent = createClass({
  render: function () {
    return (
      <div>
        <h1 className="text-center">Users Grid</h1>
        <GridComponent/>
        <Popups/>
      </div>
    );
  }
});

module.exports = AppComponent;
