'use strict';

var app = angular.module('MonitorApp');
app.controller('ServerMonitorController', function ($scope, $location, $routeParams, Monitor, Metric) {


    $scope.nav = $routeParams.nav || 'dashboard';
    $scope.template = 'views/server/' + $scope.nav + ".html";


});
app.controller('QueryMonitorController', function ($scope, $location, $routeParams, Monitor, Metric) {


    $scope.rid = $routeParams.server;
    $scope.db = $routeParams.db
    $scope.refresh = function () {

        var metricName = 'db.' + $scope.db + '.command.';
        var params = {  server: $scope.server.name, type: 'realtime', kind: 'chrono', names: metricName };
        Metric.get(params, function (data) {
            $scope.commands = $scope.flatten(data.result, metricName);


        });
    }
    Monitor.getServer($scope.rid, function (data) {
        $scope.server = data;
        $scope.findDatabases(data.name);

    });
    $scope.delete = function () {
        var metricName = 'db.' + $scope.db + '.command.';
        var params = {  server: $scope.sName, type: 'realtime', names: metricName };
        Metric.delete(params, function (data) {
            console.log(data);
        });
    }
    $scope.flatten = function (result, metricName) {
        var commands = new Array;
        result.forEach(function (elem, idx, array) {
            Object.keys(elem).forEach(function (e, i, a) {
                var obj = {};
                obj.name = e.substring(metricName.length, e.length);
                Object.keys(elem[e]).forEach(function (ele, ide, arr) {
                    obj[ele] = elem[e][ele];
                });

                commands.push(obj);
            });
        });
        return commands;
    }
    $scope.findDatabases = function (server) {
        var params = {  server: server, type: 'realtime', kind: 'information', names: 'system.databases' };
        Metric.get(params, function (data) {
            $scope.databases = data.result[0]['system.databases'].split(",");
            if ($scope.databases.length > 0 && !$scope.db) {
                $scope.db = $scope.databases[0];
                $scope.refresh();
            }
        });
    }
    $scope.$watch("db", function (data) {
        if (data)
            $location.path("/dashboard/query/" + $scope.rid + "/" + data);
    })

});
app.controller('GeneralMonitorController', function ($scope, $location, $routeParams, Monitor, Metric, Server) {


    $scope.rid = $routeParams.server;

    $scope.editorOptions = {
        lineWrapping : true,
        lineNumbers: true,
        mode: 'xml'
    };
    Monitor.getServer($scope.rid, function (data) {
        $scope.server = data;
        Server.findDatabases(data.name, function (data) {
            $scope.databases = data;
            var db = $scope.databases[0];
            $scope.dbselected = db;
        });
        Server.getConfiguration($scope.server, function (data) {
            $scope.configuration = data.configuration;
        });


    });
    $scope.saveConfig = function(){
        Server.saveConfiguration($scope.server,$scope.configuration, function (data) {
            console.log(data);
        });
    }
    $scope.getServerMetrics = function () {

        var names = new Array;
        $scope.databases.forEach(function (db, idx, array) {
            var create = 'db.' + db + '.createRecord';
            var update = 'db.' + db + '.updateRecord';
            var del = 'db.' + db + '.deleteRecord';
            var read = 'db.' + db + '.readRecord';
            names.push(create);
            names.push(update);
            names.push(del);
            names.push(read);
        });
        Metric.getMetrics({names: names, server: $scope.rid }, function (data) {
            $scope.serverLoad = new Array;
            var tmpArr = new Array;

            data.result.forEach(function (elem, idx, array) {
                var last = elem.name.lastIndexOf(".");
                var length = elem.name.length;
                elem.name = elem.name.substring(last + 1, length);
            });
            data.result.forEach(function (elem, idx, array) {
                if (!tmpArr[elem.name]) {
                    tmpArr[elem.name] = new Array;
                }
                tmpArr[elem.name].push([elem.dateTo, elem.entries]);
            });
            data.result.forEach(function (elem, idx, array) {
                if (!tmpArr[elem.name]) {
                    tmpArr[elem.name] = new Array;
                }
                tmpArr[elem.name].push([elem.dateTo, elem.entries]);
            });
            $scope.serverLoad = tmpArr;
        });
    }
    $scope.getDbMetrics = function (db) {
        var DOT = '.';
        var CREATE_LABEL = 'createRecord';
        var UPDATE_LABEL = 'updateRecord';
        var DELETE_LABEL = 'deleteRecord';
        var READ_LABEL = 'readRecord';
        var create = 'db.' + db + DOT + CREATE_LABEL;
        var update = 'db.' + db + DOT + UPDATE_LABEL;
        var del = 'db.' + db + DOT + DELETE_LABEL;
        var read = 'db.' + db + DOT + READ_LABEL;
        Metric.getMetrics({names: [create, update, read, del], server: $scope.rid }, function (data) {
            $scope.operationData = new Array;
            var tmpArr = new Array;

            data.result.forEach(function (elem, idx, array) {
                if (!tmpArr[elem.name]) {
                    tmpArr[elem.name] = new Array;
                }
                tmpArr[elem.name].push([elem.dateTo, elem.entries]);
            });
            data.result.forEach(function (elem, idx, array) {
                if (!tmpArr[elem.name]) {
                    tmpArr[elem.name] = new Array;
                }
                tmpArr[elem.name].push([elem.dateTo, elem.entries]);
            });

            $scope.operationData = tmpArr;
        });
    }
    $scope.selectDb = function (db) {
        $scope.dbselected = db;

    }
    $scope.$watch('dbselected', function (data) {

        if (data) {
            $scope.getDbMetrics(data);
        }
    });
    $scope.$watch('databases', function (data) {
        if (data)
            $scope.getServerMetrics();
    });


});
app.controller('MetricsMonitorController', function ($scope, $location, $routeParams, Monitor, Metric, Server, MetricConfig) {

    $scope.rid = $routeParams.server;
    $scope.names = new Array;
    $scope.render = 'area';
    $scope.fields = ['value', 'entries', 'min', 'max', 'average', 'total'];
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
        MetricConfig.deleteConfig(config, function (data) {
            $scope.refreshMetricConfig();
        })
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