'use strict';

var app = angular.module('MonitorApp');

app.controller('QueryMonitorController', function ($scope, $location, $routeParams, Monitor, Metric, $i18n) {


    $scope.rid = $routeParams.server;
    $scope.db = $routeParams.db;

    $scope.filterOptions = {filterText: '' };

    $scope.gridOptions = { data: 'commands',
        enablePaging: true,
        pagingOptions: $scope.pagingOptions,
        totalServerItems: 'total',
        filterOptions: $scope.filterOptions,
        columnDefs: [
            {field: 'name', displayName: $i18n.get('queryprofiler.type'), cellFilter: 'ctype'},
            {field: 'name', displayName: $i18n.get('queryprofiler.command'), cellFilter: 'cname'},
            {field: 'entries', displayName: $i18n.get('queryprofiler.entries')},
            {field: 'average', displayName: $i18n.get('queryprofiler.average')},
            {field: 'total', displayName: $i18n.get('queryprofiler.total')},
            {field: 'max', displayName: $i18n.get('queryprofiler.max')},
            {field: 'min', displayName: $i18n.get('queryprofiler.min')},
            {field: 'last', displayName: $i18n.get('queryprofiler.last')}
        ]
    };
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
        var params = {  server: $scope.server.name, type: 'realtime', names: metricName };
        Metric.delete(params, function (data) {
            $scope.refresh();
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
    $scope.$watch("server", function (server) {
        if (server) {
            $scope.findDatabases(server.name);
        }
    });
    $scope.findDatabases = function (server) {
        var params = {  server: server, type: 'realtime', kind: 'information', names: 'system.databases' };
        Metric.get(params, function (data) {
            $scope.databases = data.result[0]['system.databases'].split(",");
            if ($scope.databases.length > 0) {
                $scope.db = $scope.databases[0];
            }
            if ($scope.db) {
                $scope.refresh();
            }
        });
    }


});