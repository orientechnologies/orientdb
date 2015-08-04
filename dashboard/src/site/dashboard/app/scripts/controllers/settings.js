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
        $scope.hide();
        $scope.refresh();
      });
  }
};
module.controller('ServerNewController', function ($scope, $location, $injector, $routeParams, Monitor, Server) {


  $injector.invoke(ServerBaseController, this, {$scope: $scope});
  $scope.server = new Object;
  $scope.server['@rid'] = '#-1:-1';
  $scope.server['@class'] = 'Server';
  $scope.server.enabled = true;
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
module.controller('SettingsController', function ($scope, $location, $injector, $routeParams, Metric, MetricConfig, CommandLogApi, Settings) {

  $scope.currentTab = 'metrics';


  $scope.filterOptions = {filterText: ''};
  $scope.metricsModified = {};
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
  $scope.gridOptions = {
    data: 'metrics',
    enablePaging: true,
    pagingOptions: $scope.pagingOptions,
    totalServerItems: 'total',
    filterOptions: $scope.filterOptions,
    columnDefs: [
      {field: 'name', displayName: 'Name'},
      {field: 'description', displayName: 'Description'},
      {
        field: 'enabled',
        displayName: 'Enabled',
        enableCellEdit: true,
        cellTemplate: '<input type="checkbox" ng-change="modifiedMetrics(row.entity)" ng-model="row.entity.enabled">'
      }
    ]
  };

  $scope.modifiedMetrics = function (row) {
    if ($scope.metricsModified[row.name] == undefined) {
      $scope.metricsModified[row.name] = new Array(row);
    }
  }

  Metric.getMetricDictionary(function (data) {
    $scope.metrics = data.result;
    $scope.total = $scope.metrics.length;
    console.log($scope.total);

  });
  $scope.getGridClass = function (index, gridOpt) {
    var css = '';
    css += (index % gridOpt == 0) ? 'no-margin' : '';
    css += ' span' + 12 / gridOpt;
    return css;
  }
  $scope.refreshMetricConfig = function () {
    MetricConfig.getAll(function (data) {
      var tmp = data.result;
      if (tmp.length > 0) {
        $scope.selectedConfig = tmp[0];
        if (!$scope.config['metrics']) {
          $scope.config['metrics'] = new Array;
        }
        if ($scope.config['metrics']) {
          $scope.savedMetrics = tmp.filter(function (elem) {
            var found = true;
            $scope.config['metrics'].forEach(function (el, idx, arr) {
              if (el['name'] == elem['name']) {
                found = false;
              }
            });
            return found;
          });
        }
      }
    });
  }
  $scope.saveSettings = function () {
    $scope.testMsg = null;
    Settings.put($scope.config, function (data) {
      $scope.testMsg = "Settings updated successfully.";
      $scope.testMsgClass = 'alert alert-setting'
      $scope.config = data.result[0];

      for (var elem in $scope.metricsModified) {
        MetricConfig.saveConfig($scope.metricsModified[elem][0], function (data) {
        })

      }

    }, function (error) {
      $scope.testMsg = error;
      $scope.testMsgClass = 'alert alert-error alert-setting'
    });
  }
  $scope.selectConfig = function (config) {
    if (!$scope.config['metrics']) {
      $scope.config['metrics'] = new Array;
    }
    $scope.config['metrics'].push(config);
    var idx = $scope.savedMetrics.indexOf(config);
    $scope.savedMetrics.splice(idx, 1);
  }
  $scope.deleteConfig = function (config) {
    var idx = $scope.config['metrics'].indexOf(config);
    $scope.config['metrics'].splice(idx, 1);
    $scope.savedMetrics.push(config);

  }

  $scope.getMailSettings = function () {

    $scope.mailproperties = $scope.config['mailProfile'];

  }
  $scope.getHoursDelete = function () {


    if ($scope.config['deleteMetricConfiguration'] == undefined) {
      $scope.config['deleteMetricConfiguration'] = {};
      $scope.config['deleteMetricConfiguration']['@class'] = 'DeleteMetricConfiguration';
      $scope.config['deleteMetricConfiguration']['@type'] = 'document';
      $scope.config['deleteMetricConfiguration']['hours'] = 0;
    }

    if ($scope.config['proxyConfiguration'] == undefined) {
      $scope.config['proxyConfiguration'] = {};
      $scope.config['proxyConfiguration']['@class'] = 'ProxyConfiguration';
      $scope.config['proxyConfiguration']['@type'] = 'document';
      $scope.config['proxyConfiguration']['proxyIp'] = undefined;
      $scope.config['proxyConfiguration']['proxyPort'] = undefined;
    }
    if ($scope.config['notificationsConfiguration'] == undefined) {
      $scope.config['notificationsConfiguration'] = {};
      $scope.config['notificationsConfiguration']['@class'] = 'NotificationsConfiguration';
      $scope.config['notificationsConfiguration']['@type'] = 'document';
      $scope.config['notificationsConfiguration']['hours'] = 0;
    }
    if ($scope.config['updateConfiguration'] == undefined) {
      $scope.config['updateConfiguration'] = {};
      $scope.config['updateConfiguration']['@class'] = 'UpdateConfiguration';
      $scope.config['updateConfiguration']['@type'] = 'document';
      $scope.config['updateConfiguration']['hours'] = 24;
      $scope.config['updateConfiguration']['receiveNews'] = true;
    }


    $scope.deleteMetricConfiguration = $scope.config['deleteMetricConfiguration'];
    $scope.notificationsConfiguration = $scope.config['notificationsConfiguration'];
    $scope.proxyConfiguration = $scope.config['proxyConfiguration'];
    $scope.updateConfiguration = $scope.config['updateConfiguration'];


  }
  $scope.$watch("config", function (data) {
    if (data) {
      $scope.refreshMetricConfig();
      $scope.getMailSettings();
      $scope.getHoursDelete();
    }
  });
  $scope.purgeMetrics = function () {
    CommandLogApi.purge({type: 'metrics'}, function (data) {

      }
    );
  }
  $scope.purgeLogs = function () {
    CommandLogApi.purge({type: 'logs'}, function (data) {
      }
    );
  }
});

function validateIpAndPort(input) {
  var parts = input.split(":");
  var ip = parts[0].split(".");
  var port = parts[1];
  return validateNum(port, 1, 65535) && ip.length == 4 && ip.every(function (segment) {
      return validateNum(segment, 0, 255);
    });
}

function validateNum(input, min, max) {
  var num = +input;
  return num >= min && num <= max && input === num.toString();
}
