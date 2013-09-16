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
