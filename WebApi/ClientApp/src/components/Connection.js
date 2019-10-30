import React, { Component } from 'react';
import { Link } from 'react-router-dom';
import { Button } from 'reactstrap';
import model from '../model';

export class Connection extends Component {
    constructor(p) {
        super(p);

        this.onChange = this.onChange.bind(this);
    }

    onChange(e) {
        model[e.target.name] = e.target.value;
        this.forceUpdate();
    }

    render() {
        return (
            <div>
                <h1>Settings</h1>

                <div className="form-group">
                    <label htmlFor="host">IP/HOST</label>
                    <input name="host" value={model.host} className="form-control" onChange={this.onChange} />
                </div>

                <div className="form-group">
                    <label htmlFor="port">Port</label>
                    <input name="port" value={model.port} type="number" className="form-control" onChange={this.onChange} />
                </div>

                <div className="form-group">
                    <label htmlFor="username">User</label>
                    <input name="username" value={model.username} className="form-control" onChange={this.onChange} />
                </div>

                <div className="form-group">
                    <label htmlFor="password">Password</label>
                    <input name="password" value={model.password} type="password" className="form-control" onChange={this.onChange} />
                </div>

                <div className="form-group">
                    <label htmlFor="database">User</label>
                    <input name="database" value={model.database} className="form-control" onChange={this.onChange} />
                </div>

                <Button tag={Link} className="btn btn-primary" to="/query">Query ></Button>
            </div>
        );
    }
}