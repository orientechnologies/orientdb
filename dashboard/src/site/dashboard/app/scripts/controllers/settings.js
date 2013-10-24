'use strict';

var module = angular.module('MonitorApp');
module.controller('ServersController', function ($scope, $location, $routeParams, Monitor) {

    Monitor.serverStatus(function (data) {
        $scope.servers = data.result;
    })
});

function ServerBaseController($scope, $location, $routeParams, Monitor) {

    $scope.save = function () {
        Monitor.updateServer($scope.server, function (data) {
            $scope.refresh();
        });
    }
};
module.controller('ServerNewController', function ($scope, $location, $injector, $routeParams, Monitor, Server) {


    $injector.invoke(ServerBaseController, this, {$scope: $scope});
    $scope.server = new Object;
    $scope.server['@rid'] = '#-1:-1';
    $scope.server['@class'] = 'Server';

    $scope.test = function () {
        Server.isAlive($scope.server, function (data) {
            $scope.testMsg = 'Connection is alive';
            $scope.testMsgClass = 'alert alert-success'
        }, function (data) {
            $scope.testMsg = 'No connection';
            $scope.testMsgClass = 'alert alert-error'

        });
    }
});
module.controller('ServerEditController', function ($scope, $location, $injector, $routeParams, Monitor) {

    $injector.invoke(ServerBaseController, this, {$scope: $scope});
    Monitor.getServer($scope.serverID, function (data) {
        $scope.server = data;
    });

});
module.controller('SettingsController', function ($scope, $location, $injector, $routeParams, Metric, MetricConfig, Settings) {

    $scope.currentTab = 'dashboard';

    Settings.get(function (data) {
        if (data.result.length == 0) {
            $scope.config = Settings.new();
        } else {
            $scope.config = data.result[0];
        }


    });
    $scope.pagingOptions = {
        pageSizes: [10, 20, 50],
        pageSize: 10,
        currentPage: 1
    };
    $scope.gridOptions = { data: 'metrics',
        enablePaging: true,
        pagingOptions: $scope.pagingOptions,
        totalServerItems: 'total',
        columnDefs: [
            {field: 'name', displayName: 'Name'},
            {field: 'description', displayName: 'Age'},
            {field: 'enabled', displayName: 'Enabled'}
        ]
    };
    Metric.getMetricTypes(null, function (data) {
        $scope.metrics = data.result;
        $scope.total = $scope.metrics.length;

    });

    $scope.refreshMetricConfig = function () {
        MetricConfig.getAll(function (data) {
            $scope.savedMetrics = data.result;
            if ($scope.savedMetrics.length > 0) {
                $scope.selectedConfig = $scope.savedMetrics[0];
            }
        });
    }
    $scope.saveSettings = function () {
        Settings.put($scope.config,function(data){
            $scope.config = data.result[0];
        });
    }
    $scope.selectConfig = function (config) {
        if (!$scope.config['metrics']) {
            $scope.config['metrics'] = new Array;
        }
        $scope.config['metrics'].push(config);
    }
    $scope.deleteConfig = function (config) {
        if (!$scope.config['metrics']) {

        }
        var idx =$scope.config['metrics'].indexOf(config);
        $scope.config['metrics'].splice(idx,1);

    }

    $scope.refreshMetricConfig();
});