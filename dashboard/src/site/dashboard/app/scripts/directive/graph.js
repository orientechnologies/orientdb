var Widget = angular.module('monitor.gdirective', []);


Widget.directive('servergraph', function () {

    var drawGrap = function (scope, element, attrs, model) {

        var dbHeight = 80;

        element.empty();
        var width = 960,
            height = 400,
            colors = d3.scale.category10();
        var svg = d3.select(element[0]).append('svg').attr('width', width)
            .attr('height', height);

        var selected_node = null;


        var nodes = [];
        var links = [

        ];
        var databasesId = [];
        model.forEach(function (val, idx, arr) {

            nodes.push({ id: idx, reflexive: idx == 0, el: val, x: 400, y: 500});

        });
        var size = model.length;
        if (size == 0) return;
        model.forEach(function (server, idx, arr) {


            server.databases.forEach(function (db, dbIdx) {
                if (!databasesId[db]) {
                    var id = size + dbIdx;
                    databasesId[db] = id;
                    nodes.push({ id: id, reflexive: true, el: { name: db, db: true}, x: 400, y: 500});
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
            .linkDistance(200)
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
        g.on("click", function (d) {
            scope.$broadcast('dbselected', d);
        });
        g.each(function (el) {
            var thisGroup = d3.select(this);

            function appendDB() {
                thisGroup.append('svg:rect')
                    .attr('class', 'rectdb')
                    .attr('width', dbHeight)
                    .attr('height', dbHeight)
                    .style('fill', function (d) {
                        return '#c5c5c5';
                    })
                    .style('stroke', function (d) {
                        return d3.rgb(colors(d.id)).darker().toString();
                    })
                    .classed('reflexive', function (d) {
                        return d.reflexive;
                    });

                thisGroup.append('svg:text')
                    .attr('x', dbHeight / 2)
                    .attr('y', dbHeight / 2)
                    .attr('class', 'id')
                    .text(function (d) {
                        return d.el.name;
                    });
            }

            function appendServer() {
                thisGroup.append('svg:circle')
                    .attr('class', 'node')
                    .attr('r', 40)
                    .attr("fixed", true)
                    .style('fill', function (d) {

                        return (d.el.status === "ONLINE") ? "#468847" : "#B94A48";
                    })
                    .style('stroke', function (d) {
                        return (d.el.status === "ONLINE") ? "#468847" : "#B94A48";
                    })
                    .classed('reflexive', function (d) {
                        return d.reflexive;
                    });
                thisGroup.append('svg:text')
                    .attr('x', 0)
                    .attr('y', 4)
                    .attr('class', 'id')
                    .text(function (d) {
                        return d.el.name;
                    });
            }

            if (!el.el.db) {
                appendServer();
            } else {
                appendDB();
            }

        });


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
                return 'translate(' + d.x + ',' + d.y + ')';
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

        var sizeC = Object.keys(model.config.clusters).length * 50;
        var margin = {top: 20, right: 120, bottom: 20, left: 40},
            width = 400 - margin.right - margin.left,
            height = (sizeC + 400) - margin.top - margin.bottom;


        var children = [];
        var keys = Object.keys(model.config.clusters);

        keys.forEach(function (val) {
            if (val.indexOf("@") != 0) {
                var servChild = undefined;
                if (model.config.clusters[val]) {
                    var servChild = [];
                    var serv = model.config.clusters[val].servers;
                    serv.forEach(function (val) {
                        servChild.push({ name: val });
                    });

                }
                children.push({name: val, children: servChild});
            }
        });
        var root = {name: model.name, children: children};


        var svg = d3.select(element[0]).append('svg')
            .attr("width", width + margin.right + margin.left)
            .attr("height", height + margin.top + margin.bottom)
            .append("g")
            .attr("transform", "translate(" + margin.left + "," + margin.top + ")");


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
                return "translate(" + d.y + "," + d.x + ")";
            });

        node.append("svg:circle")
            .attr("r", 30);

        node.append("svg:text")
            .attr("x", 0)
            .attr("y", 4)
            .attr('class', 'id')
            .text(function (d) {
                return d.name;
            });
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
