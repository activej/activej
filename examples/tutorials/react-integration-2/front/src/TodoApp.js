import React from 'react';
import './index.css';
import TodoService from './TodoService.js';
import TodoList from './TodoList.js';

class TodoApp extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            text: '',
            plan1: '',
            plan2: '',
            records: []
        };
    }

    componentDidMount() {
        TodoService.loadList()
            .then(data => this.setState({records: data}))
    }

    handleDelete = (index) => {
        TodoService.delete(index)
            .then(this.handleLoad);
    };

    handleToggle = (index, planId) => {
        TodoService.toggle(index, planId)
            .then(this.handleLoad);
    };

    handleLoad = () => {
        TodoService.loadList()
            .then(data => {
                this.setState({
                    text: '',
                    plan1: '',
                    plan2: ''
                });
                this.setState({records: data});
            });
    };

    handleSubmit = (e) => {
        e.preventDefault();
        if (!this.state.text.length &&
            !this.state.plan1.length &&
            !this.state.plan2.length) {
            return;
        }

        const newItem = {
            title: this.state.text,
            plans: [this.state.plan1, this.state.plan2]
        };

        TodoService.sendNewRecord(newItem)
            .then(this.handleLoad);
    };

    handleChangeTitle = (e) => {
        this.setState({text: e.target.value});
    };

    handleChangePlan1 = (e) => {
        this.setState({plan1: e.target.value});
    };

    handleChangePlan2 = (e) => {
        this.setState({plan2: e.target.value});
    };


    render() {
        return (
            <div>
                <h3>TODO</h3>
                <TodoList onDelete={this.handleDelete}
                          onToggle={this.handleToggle}
                          records={this.state.records}/>

                <br/><hr/>
                <form onSubmit={this.handleSubmit}>
                    <table>
                        <tr>
                            <th>
                                <label htmlFor="new-todo">
                                    What needs to be done?
                                </label>
                            </th>
                            <th>
                                <input
                                    id="new-todo"
                                    onChange={this.handleChangeTitle}
                                    value={this.state.text}
                                />
                            </th>
                        </tr>

                        <tr>
                            <th>Plan1</th>
                            <th>
                                <input id="plan1"
                                       onChange={this.handleChangePlan1}
                                       value={this.state.plan1}
                                />
                            </th>
                        </tr>

                        <tr>
                            <th>Plan2</th>
                            <th>
                                <input id="plan2"
                                       onChange={this.handleChangePlan2}
                                       value={this.state.plan2}
                                />
                            </th>
                        </tr>
                    </table>
                    <button>
                        Add new one
                    </button>
                </form>
            </div>
        );
    }
}

export default TodoApp;
