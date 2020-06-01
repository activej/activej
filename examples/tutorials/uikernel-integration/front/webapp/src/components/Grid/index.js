'use strict';

var UIKernel = require('uikernel');
var React = require('react');
var model = require('../../models/users/client');
var columns = require('./columns');
var FiltersForm = require('../FiltersForm');
var RecordForm = require('../RecordForm');
var Popups = require('../../common/popup');
var createClass = require('create-react-class');

var GridComponent = createClass({
  getInitialState: function () {
    return {model: model};
  },
  applyFilters: function (filters) {
    this.setState({model: UIKernel.applyGridFilters(model, filters)});
  },
  onAdd: function () {
    var popup = Popups.open(RecordForm, {
      model: new UIKernel.Adapters.Grid.ToFormCreate(this.refs.grid.getModel(), {
        name: '',
        phone: '',
        age: '',
        gender: 'MALE'
      }),
      mode: 'create',
      onSubmit: function (newRecordId) {
        popup.close();
        this.refs.grid.addRecordStatus(newRecordId, 'new');
      }.bind(this),
      onClose: function () {
        popup.close();
      }
    });
  },
  onSave: function () {
    this.refs.grid.save(function (err) {
      if (err) {
        return window.toastr.error(err.message);
      }
    });
  },
  onClear: function () {
    this.refs.grid.clearAllChanges();
  },
  render: function () {
    return (
      <div className="panel">
        <div className="panel-heading">
          <FiltersForm onChange={this.applyFilters}/>
        </div>
        <div className="panel-body padding0">
          <UIKernel.Grid
            ref="grid"
            model={this.state.model}
            cols={columns}
            viewCount={10}
          />
        </div>
        <div className="panel-footer">
          <a className="btn btn-primary" onClick={this.onAdd}>
            Add
          </a>
          <div className="pull-right">
            <a className="btn btn-success" onClick={this.onClear}>
              Clear
            </a>
            {' '}
            <a className="btn btn-primary" onClick={this.onSave}>
              Save
            </a>
          </div>
        </div>
      </div>
    );
  }
});

module.exports = GridComponent;
