var graph = angular.module('graph', []);


graph.directive('ngGraphQuery', function () {
  var width = 960,
    height = 300;
  var colors = d3.scale.category10();
  var linker = function (scope, element, attrs) {

    var opts = angular.extend({}, scope.$eval(attrs.ngGraphQuery));

    var links = [];
    var nodes = [];

    opts.data.forEach(function (elem) {
      nodes.push(elem);

      var keys = Object.keys(elem);

      keys.forEach(function (k) {
        if (elem[k] instanceof Array) {

          elem[k].forEach(function (rid) {
            if (typeof  rid == "string") {

              nodes.push({'@rid': rid})
            } else {
              nodes.push(rid);
            }

          })
        }
      });
    });
    var force = d3.layout.force()
      .nodes(nodes)
      .links(links)
      .size([width, height])
      .linkDistance(150)
      .charge(-500)
      .on('tick', tick)


    var svg = d3.select(element[0])
      .append('svg')
      .attr('width', width)
      .attr('height', height);


    var path = svg.append('svg:g').selectAll('path');
    var circle = svg.append('svg:g').selectAll('g');

    function tick() {
      circle.attr('transform', function (d) {
        return 'translate(' + d.x + ',' + d.y + ')';
      });
    }

    path = path.data(links);

    circle = circle.data(nodes, function (d) {
      return d['@rid'];
    });

    circle.selectAll('circle').style('fill', function (d) {
      return "yellow";
    })

    var g = circle.enter().append('svg:g');

    g.append('svg:circle')
      .attr('class', 'node')
      .attr('r', 20)
      .style('fill', function (d) {
        return colors(d.id);
      })
      .style('stroke', function (d) {
        return d3.rgb(colors(d.id)).darker().toString();
      })

    g.append('svg:text')
      .attr('x', 0)
      .attr('y', 4)
      .attr('class', 'id')
      .text(function (d) {
        return d['@rid'];
      });

    force.start();
  }

  return {
    // A = attribute, E = Element, C = Class and M = HTML Comment
    restrict: 'A',
    link: linker
  }
});

graph.directive('ngOGraph', function () {

  var linker = function (scope, element, attrs) {


    var opts = angular.extend({}, scope.$eval(attrs.ngOGraph));

    scope.$watch(attrs.ngOGraph, function (data) {

      if (data) {
        loadGraph();
      }
    })
    function loadGraph() {
      var opts = angular.extend({}, scope.$eval(attrs.ngOGraph));
      var ograph = OrientGraph.create(element[0], opts.config, opts.metadata, opts.menu, opts.edgeMenu);
      ograph.data(opts.data).draw();

      if (opts.onLoad) {
        opts.onLoad(ograph);
      }
    }

    if (opts.config) {
      loadGraph();
    }
  }
  return {
    restrict: 'A',
    link: linker
  }
});
graph.directive('aside', function ($http, $compile) {

  var linker = function (scope, element, attrs) {


    scope.$watch("model.loading", function (s) {


      if (s) {
        $http.get(scope.model.tpl).then(function (response) {
          var el = angular.element($compile(response.data)(scope.model.scope));
          element.empty();
          element.append(el);
          scope.model.loading = false;
        });


      }
    })


  }
  return {
    restrict: 'A',
    link: linker
  }
});


graph.directive('c3chart', function ($http, $compile, $timeout, $rootScope) {

  var linker = function (scope, element, attrs) {


    var lastC = null;
    var lastR = null;
    var lastU = null;
    var lastD = null;


    scope.$watch("server", function (server) {

      if (server) {
        startChart();
      }
    })


    var startChart = function () {
      var counter = 0;
      var length = 0;
      var limit = 60;
      var tail = 1;
      element.id = scope.server.name;


      scope.chart = c3.generate({
        bindto: "#" + element.id,
        data: {
          x: 'x',
          columns: [
            ['x', new Date()],
            ['Create', 0],
            ['Read', 0],
            ['Update', 0],
            ['Delete', 0],
          ]
        },
        point: {
          show: false
        },
        size: {
          height: 250
        },
        axis: {
          x: {
            type: 'timeseries',
            tick: {
              culling: {
                max: 4 // the number of tick texts will be adjusted to less than this value
              },
              format: '%H:%M:%S',
            }
          },
          y: {}
        }
      });

      var countOps = function (array, regexp) {

        var keys = Object.keys(array).filter(function (k) {
          return k.match(regexp) != null;
        })

        var ops = 0;
        keys.forEach(function (k) {
          ops += array[k].entries;
        });
        return ops;
      }
      $rootScope.$on('server:updated', function (evt, server) {
        if (!server.name || server.name == scope.server.name) {

          var cOps = countOps(server.realtime['chronos'], /db.*createRecord/g);

          var rOps = countOps(server.realtime['chronos'], /db.*readRecord/g);

          var uOps = countOps(server.realtime['chronos'], /db.*updateRecord/g);

          var dOps = countOps(server.realtime['chronos'], /db.*deleteRecord/g);


          var createOperation = 0;
          var readOperation = 0;
          var updateOperation = 0;
          var deleteOperation = 0;

          if (lastC != null) {
            createOperation = Math.abs(lastC - cOps) / (POLLING / 1000);
          }
          lastC = cOps;
          if (lastR != null) {
            readOperation = Math.abs(lastR - rOps) / (POLLING / 1000);
          }
          lastR = rOps;
          if (lastU != null) {
            updateOperation = Math.abs(lastU - uOps) / (POLLING / 1000);
          }
          lastU = uOps;
          if (lastD != null) {
            deleteOperation = Math.abs(lastD - dOps) / (POLLING / 1000);
          }
          lastD = dOps;


          if (counter == limit) {
            length = tail;
            counter -= tail;
          } else {
            length = 0;
          }

          scope.chart.flow({
            columns: [
              ['x', new Date()],
              ['Create', Math.round(createOperation)],
              ['Read', Math.round(readOperation)],
              ['Update', Math.round(updateOperation)],
              ['Delete', Math.round(deleteOperation)],
            ],
            length: length,
            duration: 1500
          })
          counter++;
          //if (counter == 60) {
          //  counter = 0;
          //  //lastC = null;
          //  //lastR = null;
          //  //lastU = null;
          //  //lastD = null;
          //  //scope.chart.load({
          //  //  columns: [
          //  //    ['x', new Date()],
          //  //    ['Create', 0],
          //  //    ['Read', 0],
          //  //    ['Update', 0],
          //  //    ['Delete', 0],
          //  //  ],
          //  //  unload: ['x', "Create", "Read", "Update", "Delete"]
          //  //
          //  //})
          //}
        }
      })
    }
  }
  return {
    restrict: 'A',
    link: linker
  }
});


graph.directive('c3Gauge', function ($http, $compile, $timeout, $rootScope) {
  var linker = function ($scope, $element, $attrs) {


    $timeout(function () {
      $scope.chart = c3.generate({
        bindto: "#" + $element[0].id,
        data: {
          columns: [
            ['data', 0]
          ],
          type: 'gauge'
        },
        color: {
          pattern: ['#60B044', '#F6C600', '#F97600', '#FF0000'], // the three color levels for the percentage values.
          threshold: {
            values: [30, 60, 90, 100]
          }
        },
        size: {
          height: $scope.height || 150
        }
      });
      $scope.$watch('value', function (data) {
        if (data) {
          $scope.chart.load({
            columns: [['data', data]]
          });
        }
      })
      $scope.$watch('height', function (data) {
        if (data) {
          $scope.chart.resize({height: data});
        }
      });
    }, 0);


  }
  return {
    restrict: 'A',
    scope: {
      value: '=value',
      height: '=height'
    },
    link: linker
  }
});
