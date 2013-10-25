'use strict';

var app = angular.module('MonitorApp');
app.controller('ServerMonitorController', function ($scope, $location, $routeParams, Monitor, Metric) {


    $scope.nav = $routeParams.nav || 'dashboard';
    $scope.template = 'views/server/' + $scope.nav + ".html";


});

app.controller('GeneralMonitorController', function ($scope, $location, $routeParams, Monitor, Metric, Server) {


    $scope.rid = $routeParams.server;


    $scope.currentTab = 'overview';
    Monitor.getServers(function (data) {
        $scope.servers = data.result;

        if (!$scope.rid && $scope.servers.length > 0) {
            $scope.rid = $scope.servers[0]['@rid'];
            $scope.server =  $scope.servers[0];
        }
        Server.findDatabases($scope.server.name, function (data) {
            $scope.databases = data;
            var db = $scope.databases[0];
            $scope.dbselected = db;
        });
        Server.getConfiguration($scope.server, function (data) {
            $scope.configuration = data.configuration;
        });
    });
    $scope.editorOptions = {
        lineWrapping: true,
        lineNumbers: true,
        mode: 'xml'
    };

    $scope.saveConfig = function () {
        Server.saveConfiguration($scope.server, $scope.configuration, function (data) {
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
        Metric.getMetrics({names: names, server: $scope.rid , limit : 20 }, function (data) {
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
        Metric.getMetrics({names: [create, update, read, del], server: $scope.rid,limit : 20 }, function (data) {
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
