var Widget = angular.module('monitor.gdirective', []);


Widget.directive('servergraph', function () {

  var m = [40, 240, 40, 240]
  var drawGrap = function (scope, element, attrs, model) {

    var dbHeight = 95;
    var serverHeight = 85;

    element.empty();
    var width = 960,
      height = 400,
      colors = d3.scale.category10();
    var svg = d3.select(element[0]).append('svg').attr('width', width)
      .attr('height', height);

    var selected_node = null;


    var nodes = [];
    var links = [];
    var databasesId = [];
    model.forEach(function (val, idx, arr) {

      nodes.push({id: idx, reflexive: idx == 0, el: val, x: 400, y: 500});

    });
    var size = model.length;
    if (size == 0) return;
    model.forEach(function (server, idx, arr) {


      server.databases.forEach(function (db, dbIdx) {
        if (!databasesId[db]) {
          var id = size + dbIdx;
          databasesId[db] = id;
          nodes.push({id: id, reflexive: true, el: {name: db, db: true}, x: 400, y: 500});
        }
      })


    });

    model.forEach(function (server, idx, arr) {
      server.databases.forEach(function (db) {
        var idDb = databasesId[db];
        links.push({"source": idx, "target": idDb, "value": 5})
      })

    })
    var force = d3.layout.force()
      .nodes(nodes)
      .links(links)
      .size([width, height])
      .charge(-500)
      .linkDistance(300)
      .on('tick', tick);


    var link = svg.selectAll(".link")
      .data(links)
      .enter().append("line")
      .attr("class", "link")
      .style("stroke-width", function (d) {
        return Math.sqrt(d.value);
      });
    var circle = svg.append('svg:g').selectAll('g');

    circle = circle.data(nodes, function (d) {
      return d.id;
    });

    circle.selectAll('circle')
      .style('fill', function (d) {
        return (d === selected_node) ? d3.rgb(colors(d.id)).brighter().toString() : colors(d.id);
      })
      .classed('reflexive', function (d) {
        return d.reflexive;
      });

    var g = circle.enter().append('svg:g');
    g.call(force.drag);

    g.each(function (el) {
      var thisGroup = d3.select(this);

      function appendDB() {

        d3.xml("img/database.svg", "image/svg+xml", function (error, documentFragment) {
          if (error) {
            console.log(error);
            return;
          }
          var svgNode = documentFragment
            .getElementsByTagName("svg")[0];
          svgNode.children[0].children[0].setAttribute("transform", "scale(2.5,2.5)");
          thisGroup.node().appendChild(svgNode.children[0]);
          thisGroup.append('svg:text')
            .attr('x', dbHeight / 2)
            .attr('y', dbHeight)
            .attr('class', 'id')
            .text(function (d) {
              return d.el.name;
            }).on("click", function (d) {
              scope.$broadcast('dbselected', d);
            });
          ;
        });


      }

      function appendServer() {

        d3.xml("img/server.svg", "image/svg+xml", function (error, documentFragment) {
          if (error) {
            console.log(error);
            return;
          }
          var svgNode = documentFragment
            .getElementsByTagName("svg")[0];
          svgNode.children[0].setAttribute("transform", "scale(3,3)");
          thisGroup.append('svg:g').node().appendChild(svgNode.children[0]);
          thisGroup.append('svg:text')
            .attr('x', (serverHeight / 2) - 12.5)
            .attr('y', serverHeight)
            .attr('class', 'id')
            .text(function (d) {
              return d.el.name + " (" + d.el.status + ") ";
            }).on("click", function (d) {
              scope.$broadcast('dbselected', d);
            });
          ;
        });


      }

      if (!el.el.db) {
        appendServer();
      } else {
        appendDB();
      }

    });

    var selSvl = d3.select("svg");
    selSvl.call(d3.behavior.zoom()
      .scaleExtent([0.5, 5])
      .on("zoom", zoom));

    function zoom() {

    }

    // remove old nodes
    function tick() {

      link.attr("x1", function (d) {
        return d.source.x;
      })
        .attr("y1", function (d) {
          return d.source.y;
        })
        .attr("x2", function (d) {
          return d.target.x + dbHeight / 2;
        })
        .attr("y2", function (d) {
          return d.target.y + dbHeight / 2;
        });

      circle.attr('transform', function (d) {

        var x = d.el.db ? d.x : d.x - 30;
        var y = d.el.db ? d.y : d.y - 45;
        return 'translate(' + x + ',' + y + ')';
      });
    }

    circle.exit().remove();

    setTimeout(function () {
      var n = 100;
      force.start();
      for (var i = n * n; i > 0; --i) force.tick();
      force.stop();
    })

  }
  return {
    require: 'ngModel',
    link: function (scope, element, attrs, modelCtrl) {
      scope.$watch(function () {
        return modelCtrl.$modelValue;
      }, function (modelValue, newVal) {
        if (modelValue) {
          drawGrap(scope, element, attrs, modelValue);
        }

      })

    }
  };
});

Widget.directive('dbgraph', function () {

  var drawDbGraph = function (scope, element, attrs, model) {
    element.empty();

    var dbHeight = 95;
    var serverHeight = 85;
    var clusterHeight = 85;

    var m = [20, 120, 20, 40]


    var children = [];
    console.log(model.config.clusters);
    var keys = Object.keys(model.config.clusters);


    var status = []
    model.servers.forEach(function (elem) {
      status[elem.name] = elem.status ? elem.status.toLowerCase() : "offline";
    })
    keys.forEach(function (val) {
      if (val.indexOf("@") != 0) {
        var servChild = undefined;
        if (model.config.clusters[val]) {
          var servChild = [];
          var serv = model.config.clusters[val].servers;
          if (serv) {
            serv.forEach(function (val) {
              servChild.push({name: val, offsetX: 40, offsetY: 10, server: true, clazz: "server-" + status[val]});
            });
          }

        }
        children.push({name: val, children: servChild, cluster: true, offsetX: 40, offsetY: 30, clazz: "cluster"});
      }
    });
    var root = {name: model.name, children: children, db: true, offsetX: 40, offsetY: 60, clazz: "db"};

    var margin = {top: 20, right: 120, bottom: 20, left: 60},
      width = 400 - margin.right - margin.left,
      height = ( 600) - margin.top - margin.bottom + (50 * (children.length + model.servers.length));

    var masterSVG = d3.select(element[0]).append('svg');
    var svg = masterSVG.attr("width", width)
      .attr("height", height)
      .append("g")
      .attr("transform", "translate(" + margin.left + "," + margin.top + ")")
      .attr("class", "drawarea");


    var cluster = d3.layout.tree()
      .size([height, width]);


    var diagonal = d3.svg.diagonal()
      .projection(function (d) {
        return [d.y, d.x];
      });

    var nodes = cluster.nodes(root);
    var links = cluster.links(nodes);

    svg.selectAll(".link-db")
      .data(links)
      .enter().append("path")
      .attr("class", "link-db")
      .attr("d", diagonal);

    var node = svg.selectAll("g.node")
      .data(nodes)
      .enter().append("svg:g")
      .attr("class", "node")
      .attr("transform", function (d) {
        return "translate(" + (d.y - d.offsetY) + "," + (d.x - d.offsetX) + ")";
      });

    node.each(function (el) {
      var thisGroup = d3.select(this);
      if (el.db) {
        d3.xml("img/database.svg", "image/svg+xml", function (error, documentFragment) {
          if (error) {
            console.log(error);
            return;
          }
          var svgNode = documentFragment
            .getElementsByTagName("svg")[0];
          svgNode.children[0].children[0].setAttribute("transform", "scale(2.2,2.2)");
          thisGroup.node().appendChild(svgNode.children[0]);
          thisGroup.append('svg:text')
            .attr('x', dbHeight / 2)
            .attr('y', dbHeight)
            .attr('class', 'id')
            .text(function (d) {
              return d.name;
            });

        });
      }
      if (el.server) {
        d3.xml("img/server.svg", "image/svg+xml", function (error, documentFragment) {
          if (error) {
            console.log(error);
            return;
          }
          var svgNode = documentFragment
            .getElementsByTagName("svg")[0];
          svgNode.children[0].setAttribute("transform", "scale(3,3)");
          thisGroup.append('svg:g').node().appendChild(svgNode.children[0]);
          thisGroup.append('svg:text')
            .attr('x', (serverHeight / 2) - 12.5)
            .attr('y', serverHeight)
            .attr('class', 'id')
            .text(function (d) {
              return d.name + " (" + status[d.name] + ") ";
            })
          ;
        });
      }
      if (el.cluster) {
        d3.xml("img/cluster.svg", "image/svg+xml", function (error, documentFragment) {
          if (error) {
            console.log(error);
            return;
          }
          var svgNode = documentFragment
            .getElementsByTagName("svg")[0];
          svgNode.children[0].setAttribute("transform", "scale(0.8,0.8)");
          thisGroup.append('svg:g').node().appendChild(svgNode.children[0]);
          thisGroup.append('svg:text')
            .attr('x', (clusterHeight / 2))
            .attr('y', clusterHeight)
            .attr('class', 'id')
            .text(function (d) {
              return d.name;
            })
          ;
        });
      }

    })
//        node.append("svg:circle")
//            .attr("r", 30)
//            .attr("class", function (d) {
//                return d.clazz ? d.clazz : "node"
//            });
//
//        node.append("svg:text")
//            .attr("x", 0)
//            .attr("y", 4)
//            .attr('class', 'id')
//            .text(function (d) {
//                return d.name;
//            });


    masterSVG.call(d3.behavior.zoom()
      .scaleExtent([0.5, 5])
      .on("zoom", zoom));

    function zoom() {
      var scale = d3.event.scale,
        translation = d3.event.translate,
        tbound = -height * scale,
        bbound = height * scale,
        lbound = (-width + m[1]) * scale,
        rbound = (width - m[3]) * scale;
      // limit translation to thresholds
      translation = [
        Math.max(Math.min(translation[0], rbound), lbound),
        Math.max(Math.min(translation[1], bbound), tbound)
      ];
      d3.select(".drawarea")
        .attr("transform", "translate(" + translation + ")" +
        " scale(" + scale + ")");
    }

  }
  return {
    require: 'ngModel',
    link: function (scope, element, attrs, modelCtrl) {
      scope.$watch(function () {
        return modelCtrl.$modelValue;
      }, function (modelValue) {

        if (modelValue) {
          drawDbGraph(scope, element, attrs, modelValue);
        }

      })

    }
  };
});
