import React, { Component } from 'react';
import { Link } from 'react-router-dom';
import { Button } from 'reactstrap';
import model from '../model';

export class Query extends Component {
    constructor(props) {
        super(props);
        this.execute = this.execute.bind(this);
    }

    execute() {
        model.execute(this.ta.value, _ => this.forceUpdate());
    }

    cancel() {
        model.cancel();
    }

    render() {
        return (
            <div>
                <textarea ref={x => this.ta = x} className="form-control" defaultValue={localStorage.getItem('q')} onChange={e => localStorage.setItem('q', e.target.value)} />

                <br />

                <Button tag={Link} className="btn-success" onClick={this.execute} to="/result">Execute</Button>

                <Button tag={Link} className="btn-danger" onClick={this.cancel} to="/query">Cancel</Button>
            </div>
        );
    }
}