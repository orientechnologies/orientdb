'use strict';

var app = angular.module('MonitorApp');

app.controller('QueryMonitorController', function ($scope, $location, $routeParams, Monitor, Metric) {


    $scope.rid = $routeParams.server;
    $scope.db = $routeParams.db;

    Monitor.getServers(function (data) {
        $scope.servers = data.result;
        if ($scope.rid) {
            $scope.servers.forEach(function (elem, idx, arr) {
                if ($scope.rid.replace("#", '') == elem['@rid'].replace("#", '')) {
                    $scope.server = elem;
                }
            });
        } else {
            if ($scope.servers.length > 0) {
                $scope.server = $scope.servers[0];
            }
        }
        if ($scope.server) {
            $scope.findDatabases($scope.server.name);
        }
    });

    $scope.refresh = function () {

        var metricName = 'db.' + $scope.db + '.command.';
        var params = {  server: $scope.server.name, type: 'realtime', kind: 'chrono', names: metricName };
        Metric.get(params, function (data) {
            $scope.commands = $scope.flatten(data.result, metricName);


        });
    }

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
            }
            if ($scope.db)
                $scope.refresh();
        });
    }


});