'use strict';

var _ = require('lodash');
var EventEmitter = require('events').EventEmitter;

function Popup() {
  this._popups = [];
  EventEmitter.call(this);
}

Popup.prototype = Object.create(EventEmitter.prototype);

Popup.prototype.constructor = Popup;

Popup.prototype.open = function (Component, props) {
  var popup = {
    Component: Component,
    props: props
  };
  this._popups.push(popup);
  this.emit('change', this._popups);
  return {
    close: this.close.bind(this, popup)
  };
};

Popup.prototype.getAll = function () {
  return this._popups;
};

Popup.prototype.close = function (popup) {
  this._popups = _.without(this._popups, popup);
  this.emit('change', this._popups);
};

Popup.prototype.closeLast = function () {
  this._popups.pop();
  this.emit('change', this._popups);
};

module.exports = new Popup();
