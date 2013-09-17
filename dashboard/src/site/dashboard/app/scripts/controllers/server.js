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

    Monitor.getServer($scope.rid, function (data) {
        $scope.server = data;
        Server.findDatabases(data.name, function (data) {
            $scope.databases = data;
            var db = $scope.databases[0];
            $scope.dbselected = db;
            var create = 'db.' + db + '.createRecord';
            var update = 'db.' + db + '.updateRecord';
            var del = 'db.' + db + '.deleteRecord';
            var read = 'db.' + db + '.readRecord';
            Metric.getOperationMetrics({names: [create, update, read, del], server: $scope.rid }, function (data) {
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

        });

    });

});
app.controller('MetricsMonitorController', function ($scope, $location, $routeParams, Monitor, Metric, Server) {

    $scope.rid = $routeParams.server;
    Metric.getMetricTypes('CHRONO', function (data) {
        $scope.metrics = data.result;
        if ($scope.metrics.length > 0) {
            var name = $scope.metrics[0].name;
            Metric.getMetrics({ name: name, server: $scope.rid}, function (data) {
                console.log(data);

            })
        }
    });
    $scope.$watch("range", function (data) {
        console.log(data);
    });

});