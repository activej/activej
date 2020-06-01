'use strict';

var UIKernel = require('uikernel');
var React = require('react');
var Popups = require('../../common/popup');
var RecordForm = require('../RecordForm');

var columns = {
  tools: {
    width: 90,
    render: [function () {
      return '<div class="text-center">\n' +
        '<a href="javascript:void(0)" ref="edit" class="text-info action">\n' +
        '<span class="glyphicon glyphicon-pencil"></span></a>\n' +
        '<a href="javascript:void(0)" ref="remove" class="text-danger action">\n' +
        '<span class="glyphicon glyphicon-remove"></span></a>\n' +
      '</div>';
    }],
    onClickRefs: {
      // ref="edit" click handler
      edit: function (event, recordId, record, grid) {
        // Create a pop-up form for editing existing records in a grid
        var popup = Popups.open(RecordForm, {
          model: new UIKernel.Adapters.Grid.ToFormUpdate(grid.getModel(), recordId),
          mode: 'edit',
          changes: grid.getRecordChanges(recordId),
          onSubmit: function () {
            popup.close();
            grid.clearRecordChanges(recordId);
          },
          onClose: function () {
            popup.close();
          }
        });
      },
      // ref="remove" click handler
      remove: function (event, recordId, record, grid) {
        grid.getModel().delete(recordId, function (err) {
          if (err) {
            window.toastr.error(err.message);
            return;
          }
          grid.updateTable();
        });

      }
    }
  },
  name: {
    name: 'Name',
    sortCycle: ['asc', 'desc', 'default'],
    // Text editor
    editor: function () {
      return <input type="text" {...this.props}/>;
    },
    render: ['name', function (record) {
      return record.name;
    }]
  },
  phone: {
    name: 'Phone',
    sortCycle: ['asc', 'desc', 'default'],
    editor: function () {
      return <input type="text" {...this.props}/>;
    },
    render: ['phone', function (record) {
      return record.phone;
    }]
  },
  age: {
    name: 'Age',
    sortCycle: ['asc', 'desc', 'default'],
    // Number editor
    editor: function () {
      return <input type="number" {...this.props}/>;
    },
    render: ['age', function (record) {
      return record.age;
    }]
  },
  gender: {
    name: 'Gender',
    sortCycle: ['asc', 'desc', 'default'],
    // Select editor
    editor: function () {
      return (
        <UIKernel.Editors.Select
          {...this.props}
          options={[
            ['MALE', 'Male'],
            ['FEMALE', 'Female']
          ]}
        />
      );
    },
    render: ['gender', function (record) {
      switch (record.gender) {
        case 'MALE': return 'Male';
        case 'FEMALE': return 'Female';
        default: return 'Undefined';
      }
    }]
  }
};

module.exports = columns;
