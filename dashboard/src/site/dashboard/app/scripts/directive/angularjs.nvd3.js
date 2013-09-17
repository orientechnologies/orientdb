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
                    return d[1];
                })
                .clipEdge(true);

            chart.xAxis
                .tickFormat(function (d) {
                    return  moment("" + d, "X").format("DD-MM-YYYY HH:mm:ss");
                });

            chart.yAxis
                .tickFormat(d3.format(',.2f'));

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
                    var keys = new Array;

                    Object.keys(data).forEach(function (elem, idx, array) {
                        data[elem].forEach(function(el,i,a){
                            if(keys.indexOf(el[0])==-1)
                                keys.push(el[0]);
                        });
                    });
                    console.log(keys)
                    keys.sort(function(a,b){
                        var aDate =  moment(a,"YYYY-MM-DD HH:mm:ss").unix();
                        var bDate =  moment(b,"YYYY-MM-DD HH:mm:ss").unix();
                        return aDate - bDate;
                    });

                    data['hidden'] = new Array;
                    keys.forEach(function(elem,idx,array){
                        data['hidden'].push([elem,0]);
                    });

                    var obj = { "key": 'hidden', "values": data['hidden'], "disabled" : true };
                    formatted.push(obj);
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


