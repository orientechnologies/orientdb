"use strict";
var core_1 = require("@angular/core");
var static_1 = require("@angular/upgrade/static");
var GraphService = core_1.Class({
    constructor: [function () {
            this.databases = {};
        }],
    init: function (db, user) {
        if (!this.databases[db])
            this.databases[db] = {};
        if (!this.databases[db][user])
            this.databases[db][user] = {};
        if (!this.databases[db][user].data) {
            this.databases[db][user].data = { vertices: [], edges: [] };
        }
    },
    query: function (db, user, q) {
        this.init(db, user);
        if (q) {
            this.databases[db][user].query = q;
        }
        return this.databases[db][user].query;
    },
    add: function (db, user, data) {
        this.init(db, user);
        this.databases[db][user].data.edges = this.databases[db][user].data.edges.concat(data.edges);
        this.databases[db][user].data.vertices = this.databases[db][user].data.vertices.concat(data.vertices);
    },
    data: function (db, user) {
        this.init(db, user);
        return this.databases[db][user].data;
    },
    clear: function (db, user) {
        this.databases[db][user].data = { vertices: [], edges: [] };
    }
});
exports.GraphService = GraphService;
angular.module('graph.services', []).factory("GraphService", static_1.downgradeInjectable(GraphService));
