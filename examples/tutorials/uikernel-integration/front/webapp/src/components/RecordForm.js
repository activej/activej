'use strict';

var React = require('react');
var UIKernel = require('uikernel');
var createClass = require('create-react-class');

var RecordForm = createClass({
  mixins: [UIKernel.Mixins.Form],

  componentDidMount: function () {
    this.initForm({
      fields: ['name', 'phone', 'age', 'gender'],
      changes: this.props.changes,
      model: this.props.model,
      submitAll: this.props.mode === 'create',
      partialErrorChecking: this.props.mode === 'create'
    })
  },

  save: function (e) {
    e.preventDefault();
    this.submit(function (err, response) {
      if (!err) {
        this.props.onSubmit(response)
      }
    }.bind(this))
  },

  render: function () {
    if (!this.isLoaded()) {
      return <span>Loading...</span>
    }

    var data = this.getData();
    var globalError = this.getGlobalError();

    return (
      <div className='modal-dialog modal-lg'>
        <div className='modal-content'>
          <div className='modal-header'>
            <button type='button' className='close' onClick={this.props.onClose}>
              <span>&times</span>
              <span className='sr-only'>Close</span>
            </button>
            <h4 className='modal-title'>
              {this.props.mode === 'edit' ? 'Edit ' + data.name : 'Create'}
            </h4>
          </div>
          <div className='modal-body'>
            <div className='form-horizontal'>
              <div className='form-group'>
                <div className='col-sm-9 col-sm-offset-3'>
                  <b className='text-danger'>{globalError ? globalError.message : ''}</b>
                </div>
              </div>
              <div className={'form-group' + (this.hasChanges('name') ? ' bg-warning' : '') +
                (this.hasError('name') ? ' bg-danger' : '')}
              >
                <label className='col-sm-3 control-label'>First Name</label>
                <div className='col-sm-9'>
                  <input
                    type='text'
                    className='form-control'
                    onChange={this.updateField.bind(null, 'name')}
                    onFocus={this.clearError.bind(null, 'name')}
                    onBlur={this.validateForm}
                    value={data.name}
                  />
                </div>
              </div>
              <div className={'form-group' + (this.hasChanges('phone') ? ' bg-warning' : '') +
                (this.hasError('phone') ? ' bg-danger' : '')}
              >
                <label className='col-sm-3 control-label'>Phone</label>
                <div className='col-sm-9'>
                  <input
                    type='text'
                    className='form-control'
                    onChange={this.updateField.bind(null, 'phone')}
                    onFocus={this.clearError.bind(null, 'phone')}
                    onBlur={this.validateForm}
                    value={data.phone}
                  />
                </div>
              </div>
              <div className={'form-group' + (this.hasChanges('age') ? ' bg-warning' : '') +
                (this.hasError('age') ? ' bg-danger' : '')}
              >
                <label className='col-sm-3 control-label'>Age</label>
                <div className='col-sm-9'>
                  <input
                    type='number'
                    className='form-control'
                    onChange={this.updateField.bind(null, 'age')}
                    onFocus={this.clearError.bind(null, 'age')}
                    onBlur={this.validateForm}
                    value={data.age}
                  />
                </div>
              </div>
              <div className={'form-group' + (this.hasChanges('gender') ? ' bg-warning' : '') +
              (this.hasError('email') ? ' bg-danger' : '')}>
                <label className='col-sm-3 control-label'>Gender</label>
                <div className='col-sm-9'>
                  <UIKernel.Editors.Select
                    className='form-control'
                    onChange={this.updateField.bind(null, 'gender')}
                    onFocus={this.clearError.bind(null, 'gender')}
                    onBlur={this.validateForm}
                    options={[
                      ['MALE', 'Male'],
                      ['FEMALE', 'Female']
                    ]}
                    value={data.gender}
                  />
                </div>
              </div>
            </div>
          </div>
          <div className='modal-footer'>
            <button type='button' className='btn btn-default' onClick={this.props.onClose}>Cancel</button>
            <button type='button' className='btn btn-default' onClick={this.clearChanges}>Discard</button>
            <button type='submit' className='btn btn-primary' onClick={this.save}>Save</button>
          </div>
        </div>
      </div>
  )
  }
});

module.exports = RecordForm;
