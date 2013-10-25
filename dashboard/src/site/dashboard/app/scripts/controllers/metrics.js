'use strict';

var app = angular.module('MonitorApp');
app.controller('SingleMetricController', function ($scope, $location, $routeParams, Monitor, Metric) {

    $scope.render = "bar";


    $scope.metricScope.$watch($scope.metric, function (data) {
        $scope.config = data;
        if ($scope.range)
            $scope.refreshData(data, $scope.range.start.format("YYYY-MM-DD HH:mm:ss"), $scope.range.end.format("YYYY-MM-DD HH:mm:ss"));

    });
    //$scope.refreshData($scope.config, $scope.range.start.format("YYYY-MM-DD HH:mm:ss"), $scope.range.end.format("YYYY-MM-DD HH:mm:ss"));
    $scope.$watch('range', function (data) {
    });
    $scope.refreshData = function (metrics, dataFrom, dataTo) {

        var names = new Array;
        var configs = new Array;
        metrics.config.forEach(function (elem, idx, array) {
            names.push(elem.name);
            configs[elem.name] = elem.field;
        });
        Metric.getMetrics({ names: names, server: metrics.server, dateFrom: dataFrom, dateTo: dataTo}, function (data) {
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

app.controller('MetricsMonitorController', function ($scope, $location, $routeParams, $odialog, Monitor, Metric, Server, MetricConfig) {

    $scope.rid = $routeParams.server;
    $scope.names = new Array;
    $scope.render = 'bar';
    $scope.fields = ['value', 'entries', 'min', 'max', 'average', 'total'];

    Monitor.getServers(function (data) {
        $scope.servers = data.result;
    });
    Metric.getMetricTypes(null, function (data) {
        $scope.metrics = data.result;
        if ($scope.metrics.length > 0) {
            $scope.metric = $scope.metrics[0].name;

        }
    });
    $scope.refreshMetricConfig = function () {
        MetricConfig.getAll(function (data) {
            $scope.savedMetrics = data.result;
            if ($scope.savedMetrics.length > 0) {
                $scope.selectedConfig = $scope.savedMetrics[0];
            }
        });
    }


    $scope.newMetricConfig = function () {
        $scope.selectedConfig = MetricConfig.create();
    }
    $scope.saveMetricConfig = function () {
        MetricConfig.saveConfig($scope.selectedConfig, function (data) {
            $scope.refreshMetricConfig();
        });
    }

    $scope.selectConfig = function (config) {
        $scope.selectedConfig = config;
    }
    $scope.deleteConfig = function (config) {
        var msg = 'You are removing metric ' + config.name + '. Are you sure?';
        $odialog.confirm({title: 'Warning', body: msg, success: function () {
            MetricConfig.deleteConfig(config, function (data) {
                $scope.refreshMetricConfig();
            });
        }});

    }
    $scope.addConfig = function () {
        if (!$scope.selectedConfig['config']) {
            $scope.selectedConfig['config'] = new Array;
        }
        $scope.selectedConfig['config'].push({});
    }

    $scope.removeMetric = function (met) {
        var idx = $scope.selectedConfig['config'].indexOf(met);
        $scope.selectedConfig['config'].splice(idx, 1);
    }
    $scope.refreshMetricConfig();


});