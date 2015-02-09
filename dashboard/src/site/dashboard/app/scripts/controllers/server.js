'use strict';

var app = angular.module('MonitorApp');
app.controller('ServerMonitorController', function ($scope, $location, $routeParams, Monitor, Metric) {


  $scope.nav = $routeParams.nav || 'dashboard';
  $scope.template = 'views/server/' + $scope.nav + ".html";


});

app.controller('GeneralMonitorController', function ($scope, $location, $routeParams, Monitor, Metric, Server, MetricConfig, $i18n, ContextNotification, $odialog) {


  $scope.rid = $routeParams.server;


  $scope.profilerOff = {content: "The Profiler for this server is Off. Just click the switch button above."}
  $scope.error = false;
  $scope.currentTab = 'overview';
  Monitor.getServers(function (data) {
    $scope.servers = data.result;
    if (!$scope.rid && $scope.servers.length > 0) {
      $scope.rid = $scope.servers[0]['@rid'];
      $scope.server = $scope.servers[0];
    } else if ($scope.servers.length > 0) {
      $scope.servers.forEach(function (server) {
        if ($scope.rid.replace('#', '') == server['@rid'].replace('#', '')) {
          $scope.server = server;
          return;
        }
      });
    }
    $scope.attached = $scope.server.attached;
  });


  $scope.editorOptions = {
    lineWrapping: true,
    lineNumbers: true,
    mode: 'xml'
  };

  $scope.saveConfig = function () {

    $odialog.confirm({
      title: 'Warning!',
      body: "You are changing the Configuration for the server " + $scope.server.name + " . The changes will take effect after server restart. Are you sure?",
      success: function () {
        Spinner.start();
        Server.saveConfiguration($scope.server, $scope.configuration, function (data) {
          Spinner.stop();
        });
      }
    });

  }
  $scope.getServerMetrics = function () {


    var names = ["db.*.createRecord", "db.*.updateRecord", "db.*.readRecord", "db.*.deleteRecord"];

    var cfg = MetricConfig.create();
    cfg.name = $i18n.get('server.operations');
    cfg.server = $scope.server['@rid'];
    cfg.config = new Array;

    names.forEach(function (name) {
      cfg.config.push({name: name, field: 'entries'});
    })
    $scope.config = cfg;

  }

  $scope.$watch('attached', function (attached) {
    console.log(attached);
    if (attached != null && $scope.server && $scope.server.attached != attached) {
      if (attached) {
        Server.connect($scope.server).then(function () {
          $scope.server.attached = true;
          $scope.attached = true;
        });
      } else {
        Server.disconnect($scope.server).then(function () {
          $scope.server.attached = false;
          $scope.attached = false;
        });
      }
    }
  });
  $scope.$watch("server", function (server) {
    if (server) {
      $scope.attached = server.attached;
      Server.findDatabases(server.name, function (data) {
        $scope.databases = data;
        var db = $scope.databases[0];
        $scope.dbselected = db;
        if (db) {
          $scope.getDbMetrics(db);
        }

        Server.getConfiguration(server, function (data) {
          $scope.configuration = data.configuration;
        });
        $scope.error = false;
      }, function (error) {
        $scope.error = true;
        ContextNotification.push({content: error.data, error: true});
      });

    }
  });
  $scope.initMetrics = function () {
    var names = ["db.*.createRecord", "db.*.updateRecord", "db.*.readRecord", "db.*.deleteRecord"];
    var cfg = MetricConfig.create();
    cfg.name = $i18n.get('db.operations');
    cfg.server = $scope.server['@rid'];

    cfg.databases = db;
    cfg.config = new Array;

    names.forEach(function (name) {
      cfg.config.push({name: name, field: 'entries'});
    })
    $scope.configDb = cfg;
  }
  $scope.getDbMetrics = function (db) {
    var names = ["db.*.createRecord", "db.*.updateRecord", "db.*.readRecord", "db.*.deleteRecord"];
    var cfg = MetricConfig.create();
    cfg.name = $i18n.get('db.operations');
    cfg.server = $scope.server['@rid'];

    cfg.databases = db;
    cfg.config = new Array;

    names.forEach(function (name) {
      cfg.config.push({name: name, field: 'entries'});
    })
    $scope.configDb = cfg;
  }
  $scope.selectDb = function (db) {
    $scope.dbselected = db;

  }
  $scope.downloadDb = function (db) {
    $scope.dbselected = db;

    Server.backUpDb($scope.server, db);
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
