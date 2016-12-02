import OrientGraph from '../widgets/orientdb-graphviz';

let graph = angular.module('graph', []);


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
        let tpl = `
          <div ng-include="'${scope.model.tpl}'"></div>
        `
        var el = angular.element($compile(tpl)(scope.model.scope));
        element.empty();
        element.append(el);
        scope.model.loading = false;
      }
    })


  }
  return {
    restrict: 'A',
    link: linker
  }
});


graph.directive('serverChart', function ($http, $compile, $timeout, $rootScope) {

  var linker = function (scope, element, attrs) {


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


      element[0].id = scope.server.name + "_" + Math.round(Math.random() * 1000);
      if (scope.headers) {
        var columns = [['x', new Date()]];

        scope.headers.forEach(function (h) {
          columns.push([h.name, 0]);
        })
        scope.chart = c3.generate({
          bindto: "#" + element[0].id,
          data: {
            x: 'x',
            columns: columns
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

        $rootScope.$on('server:updated', function (evt, server) {
          if (!server.name || server.name == scope.server.name) {


            var columns = [];

            scope.headers.forEach(function (h) {
              var counter = h.transform(server);
              var value = 0;
              if (h.last != null) {
                value = Math.abs(h.last - counter) / (POLLING / 1000)
              }
              columns.push([h.name, Math.ceil(value)]);
              h.last = counter;
            })

            if (counter == limit) {
              length = tail;
              counter -= tail;
            } else {
              length = 0;
            }


            columns.unshift(['x', new Date()]);
            scope.chart.flow({
              columns: columns,
              length: length,
              duration: 1500
            })
            counter++;
          }
        })
      }
    }
  }
  return {
    restrict: 'A',
    scope: {
      headers: '=headers',
      server: '=server',
    },
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


graph.directive('c3DataCenter', function ($http, $compile, $timeout, $rootScope, $location) {
  var linker = function ($scope, $element, $attrs) {


    function getColor(dc, s) {


      var colors = dc.servers.filter(function (ser) {
        return ser.name === s;
      }).map(function (s) {
        switch (s.status) {
          case "ONLINE" :
            return "#5cb85c"
            break
          case "SYNCHRONIZING" :
            return "#f0ad4e"
            break
          default:
            return "#d9534f";
        }
      });
      return colors.length > 0 ? colors[0] : "#d9534f";
    };


    function changeIfMyServer(dc, s, status) {
      var found = false;
      dc.servers.forEach(function (ser) {

        if (ser.name === s) {
          found = true;
          ser.status = status;
        }
      })
      return found;
    }

    var data = $scope.dc.servers.map(function (s) {
      return [s.name, 50];
    });
    $timeout(function () {


      var chart = c3.generate({
        bindto: $element[0],
        tooltip: {
          show: false
        },
        data: {
          columns: data,
          onclick: function (d, element) {
            $location.path("/dashboard/general/" + d.id);
          },
          type: 'donut',
          color: function (color, d) {
            return getColor($scope.dc, d.id ? d.id : d);
          }
        },
        donut: {
          title: $scope.dc.name,
          label: {
            format: function (value, ratio, id) {
              return id;
            }
          }
        }
      });

      $scope.$on('server-status-change', function (evt, s) {


        if (changeIfMyServer($scope.dc, s.name, s.status)) {
          chart.unload({
            ids: [s.name]
          });
          chart.load({
            columns: [
              [s.name, 50]
            ],
          });
        }

      })
    }, 0);

  }
  return {
    restrict: 'A',
    scope: {
      dc: '=dc',
    },
    link: linker
  }
});

export default graph.name;

