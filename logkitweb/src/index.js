import React, { Component } from 'react';
import ReactDOM from 'react-dom';
import { Router, Route, hashHistory } from 'react-router'
import "./index.css"
import List from './listContainer'
import Create from './createContainer'

export class Index extends Component {
  constructor(props) {
    super(props);
    this.state = {
    };
  }

  componentWillMount() {

  }

  componentDidMount() {

  }

  componentDidUpdate(prevProps) {

  }

  componentWillReceiveProps(nextProps) {
    console.log(nextProps)
    if (nextProps.location.pathname != this.props.location.pathname) {
      console.log('test11')
      window.clearInterval(window.myInterval);
    }
  }


  render() {
    return
  }
}



ReactDOM.render(
    <Router history={hashHistory}>
      <Route path="/index" components={List}/>
      <Route path="/index/create" components={Create}/>
    </Router>,
    document.getElementById('root')
);
