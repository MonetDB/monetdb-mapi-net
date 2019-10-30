import React, { Component } from 'react';
import { Link } from 'react-router-dom';
import { Button } from 'reactstrap';
import model from '../model';

export class Result extends Component {
    constructor(props) {
        super(props);

        model.listen(_ => this.forceUpdate());
    }

    buildTable() {
        return (
            <table className='table table-striped'>
                <thead>
                    <tr>
                        {model.meta.map(x => <th>{x}</th>)}
                    </tr>
                </thead>
                <tbody>
                    {model.data.map((r, i) => <tr key={i}>{r.map(x => <td>{x}</td>)}</tr>)}
                </tbody>
            </table>
        );
    }

    render() {
        return (
            <div>
                {
                    model.estimate ?
                        [
                            <h1>
                                {(model.error ? 'Error' : 'Success') + ' (' + model.estimate + 's)'}
                                <Button tag={Link} className="btn btn-primary float-right" onClick={_ => model.reload()}>Reload</Button>
                            </h1>,
                        model.error ? <p>{model.error}</p> : this.buildTable()
                    ] :
                    <h1>Loading...</h1>
                }
            </div>
        );
    }
}