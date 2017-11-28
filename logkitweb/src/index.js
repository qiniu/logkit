import React, {Component} from 'react';
import ReactDOM from 'react-dom';
import {Router, Route, hashHistory} from 'react-router'
import "./index.css"
import List from './container/listContainer'
import CreateLogRunner from './container/createLogContainer'
import CreateMetricRunner from './container/createMetricContainer'

export class Index extends Component {
  constructor(props) {
    super(props);
    this.state = {};
  }

  componentWillMount() {

  }

  componentDidMount() {

  }

  componentDidUpdate(prevProps) {

  }

  render() {
    return
  }
}


ReactDOM.render(
    <Router history={hashHistory}>
      <Route path="/" components={List}/>
      <Route path="/index/create-log-runner" components={CreateLogRunner}/>
      <Route path="/index/create-metric-runner" components={CreateMetricRunner}/>
    </Router>,
    document.getElementById('root')
);
