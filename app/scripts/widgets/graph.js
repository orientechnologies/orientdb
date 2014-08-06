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