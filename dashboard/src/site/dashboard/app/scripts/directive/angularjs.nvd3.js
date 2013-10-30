/**
 * Created with JetBrains WebStorm.
 * User: erisa
 * Date: 09/09/13
 * Time: 21.46
 * To change this template use File | Settings | File Templates.
 */


var Widget = angular.module('ui-nvd3', []);
Widget.directive('piechart', function () {

    var createPie = function (data, element) {
        nv.addGraph(function () {
            var chart = nv.models.pieChart()
                .x(function (d) {
                    return d.label
                })
                .y(function (d) {
                    return d.value
                })
                .labelThreshold(.05)
                .donut(true);


            d3.select(element[0])
                .datum(data)
                .transition().duration(1200)
                .call(chart);

            return chart;
        });
    }

    return {
        restrict: 'A',
        link: function (scope, element, attr) {
            var data = attr.piechart;


            scope.$watch(data, function (data) {
                if (data)
                    createPie(data, element);
            })

        }
    };
});
Widget.directive('stackedchart', function () {

    var createStacked = function (data, element) {

        nv.addGraph(function () {
            var chart = nv.models.multiBarChart()
                .x(function (d) {
                    var unix = moment(d[0], "YYYY-MM-DD HH:mm:ss").format("X");
                    return new Number(unix);
                })
                .y(function (d) {
                    var ret = d[1] > 1000 ? d[1] / 1000 : d[1];

                    return ret;
                })
                .clipEdge(true);

            chart.xAxis
                .tickFormat(function (d) {
                    return  moment("" + d, "X").format("HH:mm");
                });

            chart.yAxis
                .tickFormat(d3.format(',.2f'));

            $(element[0]).html("");
            d3.select(element[0])
                .datum(data)
                .transition().duration(1200)
                .call(chart);

            nv.utils.windowResize(chart.update);
            return chart;
        });
    }

    return {
        restrict: 'A',
        link: function (scope, element, attr) {
            var data = attr.stackedchart;

            scope.$watch(data, function (data) {
                if (data) {
                    var formatted = new Array;
                    Object.keys(data).forEach(function (elem, idx, array) {
                        if (elem != "hidden") {
                            var obj = { "key": elem, "values": data[elem] };
                            formatted.push(obj)
                        }

                    });


                    createStacked(formatted, element);
                }
            })

        }
    };
});
Widget.directive('stackedarea', function () {

    var createStackedArea = function (scope, data, element, render, realtime) {

        nv.addGraph(function () {
            if (render == 'bar') {
                var chart = nv.models.multiBarChart();
            } else {
                var chart = nv.models.stackedAreaChart();
            }

            chart.x(function (d) {
                var unix = moment(d[0], "YYYY-MM-DD HH:mm:ss").format("X");
                return new Number(unix);
            })
                .y(function (d) {
                    var ret = d[1];
                    if (ret > 1000000) {
                        ret = ret / 1000000;
                    }
                    return ret;
                })
                .clipEdge(true);

            var format = realtime ? "HH:mm:ss" : "YYYY-MM-DD HH:mm";
            chart.xAxis
                .tickFormat(function (d) {
                    return  moment("" + d, "X").format(format);
                });
            chart.yAxis
                .tickFormat(d3.format(',.2f'));
            chart.showControls(false);
            $(element[0]).empty();
            var height = scope.chartHeight || '500';
            $(element[0]).attr('height', height);
            d3.select(element[0])
                .datum(data)
                .transition().duration(0)
                .call(chart);

            nv.utils.windowResize(chart.update);
            return chart;
        });
    }

    return {
        restrict: 'A',
        link: function (scope, element, attr) {
            var data = attr.stackedarea;
            var render = attr.stackedrender;
            var realtime = attr.realtime;

            var manipulateData = function (data) {
                var keys = new Array;
                Object.keys(data).forEach(function (elem, idx, array) {
                    Object.keys(data[elem]).forEach(function (el, i, a) {
                        if (keys.indexOf(el) == -1)
                            keys.push(el);
                    });
                });
                var formatted = new Array;
                Object.keys(data).forEach(function (elem, idx, array) {
                    if (elem != "hidden") {
                        var values = new Array;
                        keys.forEach(function (e, i, a) {
                            var v = data[elem][e];
                            if (!v) {
                                v = 0;
                            }
                            values.unshift([e, v]);
                        });
                        var obj = { "key": elem, "values": values };
                        formatted.push(obj)
                    }

                });
                createStackedArea(scope, formatted, element, scope[render], scope[realtime]);
            }
            scope.$watch(data, function (data) {
                if (data) {
                    manipulateData(data);
                } else {
                    $(element[0]).empty();
                }
            });
            scope.$watch(render, function (ren) {
                if (scope[data]) {
                    manipulateData(scope[data]);
                }
            })

        }
    };
});


