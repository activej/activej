'use strict';

var _ = require('lodash');
var React = require('react');
var UIKernel = require('uikernel');
var createClass = require('create-react-class');

var defaultFilters = {
  search: '',
  age: null,
  gender: 0
};

var FiltersForm = createClass({
  getInitialState: function () {
    return {
      filters: _.clone(defaultFilters)
    }
  },
  onChange: function (field, data) {
    if (field === 'search' || field === 'age') {
      data = data.target.value
    }
    this.state.filters[field] = data;
    this.props.onChange(this.state.filters)
  },
  onClear: function () {
    this.state.filters.search = '';
    this.state.filters.age = '';
    this.state.filters.gender = 0;
    this.props.onChange(defaultFilters)
  },
  render: function () {
    console.log(this.state.filters.search);
    return (
      <form className="filters-form row">
        <div className="col-sm-7">
          <label className="control-label">Search</label>
          <input
            type="text"
            className="form-control"
            onChange={this.onChange.bind(null, 'search')}
            value={this.state.filters.search}
          />
        </div>
        <div className="col-sm-2">
          <label className="control-label">Age</label>
          <input
            type="number"
            className="form-control"
            onChange={this.onChange.bind(null, 'age')}
            value={this.state.filters.age}
          />
        </div>
        <div className="col-sm-2">
          <label className="control-label">Gender</label>
          <UIKernel.Editors.Select
            className="form-control"
            onChange={this.onChange.bind(null, 'gender')}
            options={[
              [0, ''],
              ['MALE', 'Male'],
              ['FEMALE', 'Female']
            ]}
            value={this.state.filters.gender}
          />
        </div>
        <div className="col-sm-1">
          <label className="control-label">&nbsp;</label>
          <a className="btn btn-success show" onClick={this.onClear}>
            Clear
          </a>
        </div>
      </form>
    )
  }
});

module.exports = FiltersForm;
