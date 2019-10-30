import React, { Component } from 'react';
import { Route } from 'react-router';
import { Layout } from './components/Layout';
import { Connection } from './components/Connection';
import { Result } from './components/Result';
import { Query } from './components/Query';

export default class App extends Component {
  static displayName = App.name;

  render () {
    return (
      <Layout>
            <Route exact path='/' component={Connection} />
            <Route path='/query' component={Query} />
            <Route path='/result' component={Result} />
      </Layout>
    );
  }
}
