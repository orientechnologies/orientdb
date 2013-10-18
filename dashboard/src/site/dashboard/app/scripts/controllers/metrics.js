'use strict';

var app = angular.module('MonitorApp');
app.controller('SingleMetricController', function ($scope, $location, $routeParams, Monitor, Metric) {

    $scope.render = "area";


    $scope.metricScope.$watch($scope.metric, function (data) {
        $scope.config = data;
        if ($scope.range)
            $scope.refreshData(data, $scope.range.start.format("YYYY-MM-DD HH:mm:ss"), $scope.range.end.format("YYYY-MM-DD HH:mm:ss"));

    });
    $scope.$watch('range', function (data) {
        //$scope.refreshData($scope.config, $scope.range.start.format("YYYY-MM-DD HH:mm:ss"), $scope.range.end.format("YYYY-MM-DD HH:mm:ss"));
    });
    $scope.refreshData = function (metrics, dataFrom, dataTo) {

        var names = new Array;
        var configs = new Array;
        metrics.config.forEach(function (elem, idx, array) {
            names.push(elem.name);
            configs[elem.name] = elem.field;
        });
        Metric.getMetrics({ names: names, server: $scope.rid, dateFrom: dataFrom, dateTo: dataTo}, function (data) {
            $scope.metricsData = new Array;
            var tmpArr = new Array;


            data.result.forEach(function (elem, idx, array) {
                if (!tmpArr[elem.name]) {
                    tmpArr[elem.name] = new Array;
                }
                var el = undefined;

                if (configs[elem.name]) {
                    el = elem[configs[elem.name]];
                } else if (elem['class'] == 'Information') {
                    el = elem.value
                } else {
                    el = elem.entries;
                }
                tmpArr[elem.name][elem.dateTo] = el; //([elem.dateTo, el]);
            });
            $scope.metricsData = tmpArr;
        })
    }
});