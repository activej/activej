import React from 'react';
import './index.css';


class TodoList extends React.Component {
    render() {
        if (!this.props.records[0]) {
            return null;
        }

        return (
            <table key={this.props.records[0][0]}>
                <tbody>
                <tr>
                    <th>Id</th>
                    <th>Title</th>
                    <th>Plans</th>
                    <th>Delete</th>
                </tr>

                {this.props.records.map((item, index) =>
                    <tr key={index}>
                        <th>{item[0]}</th>
                        <th>{item[1].title}</th>
                        <th>
                            <ul>
                            {
                                item[1].plans.map((plan, index) =>
                                    <li>
                                        {plan.text}
                                        <input type="checkbox"
                                               defaultChecked={plan.isComplete}
                                               onChange={() =>
                                                   this.props.onToggle(item[0], index)}/>
                                    </li>
                                )
                            }
                            </ul>
                        </th>
                        <th>
                            <button onClick={() => this.props.onDelete(item[0])}>
                                Delete
                            </button>
                        </th>
                    </tr>)}
                </tbody>
            </table>
        )
    }
}

export default TodoList;
