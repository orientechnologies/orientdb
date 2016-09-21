var aside = angular.module('graph.services', []);

aside.factory('Graph', function () {

  var data = {vertices: [], edges: []};
  return {
    query: "",
    data: data,
    add: function (d) {
      this.data.edges = this.data.edges.concat(d.edges);
      this.data.vertices = this.data.vertices.concat(d.vertices);
    },
    clear: function () {
      this.data = {vertices: [], edges: []};
    },
    remove: function (d) {
      this.params.cls = "";
    }
  }
})
;
