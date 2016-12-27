import {Class} from '@angular/core';
import {downgradeInjectable} from '@angular/upgrade/static';

/**
 *  GraphService
 *
 *  Holds the Graph Editor info in Memory (Query + Data)
 */
let GraphService = Class({
  constructor: [function () {
    this.databases = {}
  }],

  init(db, user){
    if (!this.databases[db]) this.databases[db] = {}
    if (!this.databases[db][user]) this.databases[db][user] = {}
    if (!this.databases[db][user].data) {
      this.databases[db][user].data = {vertices: [], edges: []};
    }
  },

  query(db, user, q) {
    this.init(db, user);
    if (q) {
      this.databases[db][user].query = q;
    }
    return this.databases[db][user].query;
  },
  add(db, user, data) {
    this.init(db, user);
    this.databases[db][user].data.edges = this.databases[db][user].data.edges.concat(data.edges);
    this.databases[db][user].data.vertices = this.databases[db][user].data.vertices.concat(data.vertices);
  },
  data(db, user){
    this.init(db, user);
    return this.databases[db][user].data;
  },
  clear(db, user) {
    this.databases[db][user].data = {vertices: [], edges: []};
  }
});

angular.module('graph.services', []).factory(
  `GraphService`,
  downgradeInjectable(GraphService));

export  {GraphService};




