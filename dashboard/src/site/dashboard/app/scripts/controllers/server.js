'use strict';

var app = angular.module('MonitorApp');
app.controller('ServerMonitorController', function ($scope, $location, $routeParams, Monitor, Metric) {


  $scope.nav = $routeParams.nav || 'dashboard';
  $scope.template = 'views/server/' + $scope.nav + ".html";


});

app.controller('SingleServerController', function ($scope, $location, $routeParams, $timeout, Metric, $i18n, MetricConfig, $q, Server) {


  $scope.operations = 0;

  $scope.connections = 0;

  $scope.requests = 0;

  $scope.latency = 0;
  var lastOps = null;
  var lastReq = null;

  function initStats() {
    Server.getStats($scope.server).then(function (data) {

      $scope.server.status = data.result[0].status;


      if ($scope.server.status == 'ONLINE') {
        var snapshot = data.result[0]['snapshot'];
        var realtime = data.result[0]['realtime'];


        var total = snapshot['system.disk./.totalSpace'];
        var usable = snapshot['system.disk./.usableSpace'];
        $scope.diskPercent = Math.floor((100 - (usable * 100) / total));
        $scope.anotherPercent = -45;
        $scope.diskOptions = {
          barColor: '#E67E22',
          scaleColor: false,
          lineWidth: 3,
          lineCap: 'butt'
        };


        var maxMemory = realtime['hookValues']['process.runtime.maxMemory'];
        var totalMemory = realtime['hookValues']['process.runtime.totalMemory'];
        $scope.ramPercent = Math.floor(((totalMemory * 100) / maxMemory));
        $scope.anotherPercent = -45;
        $scope.ramOptions = {
          barColor: '#E67E22',
          scaleColor: false,
          lineWidth: 3,
          lineCap: 'butt'
        };


        var cpu = realtime['hookValues']['process.runtime.cpu'];
        $scope.cpuPercent = parseFloat(cpu).toFixed(2);

        $scope.cpuOptions = {
          barColor: '#E67E22',
          scaleColor: false,
          lineWidth: 3,
          lineCap: 'butt'
        };
        $scope.connections = realtime['hookValues']['server.connections.actives'];

        var keys = Object.keys(realtime['chronos']).filter(function (k) {
          return k.match(/db.*Record/g) != null;
        })
        var ops = 0;
        keys.forEach(function (k) {
          ops += realtime['chronos'][k].entries;
        });

        if (lastOps != null) {
          $scope.operations = Math.abs(lastOps - ops);
        }
        lastOps = ops;


        var req = realtime['chronos']['server.network.requests'].entries;
        if (lastReq != null) {
          $scope.requests = Math.abs(req - lastReq);
        }
        lastReq = req;

        if (realtime['chronos']['distributed.node.latency'])
          $scope.latency = realtime['chronos']['distributed.node.latency'].average;

      } else {
        $scope.operations = 0;
        $scope.connections = 0;
        $scope.requests = 0;
        $scope.latency = 0;
        $scope.diskPercent = 0;
        $scope.cpuPercent = 0;
        $scope.ramPercent = 0;
      }
    });
  }

  function initCrud() {
    var params = {
      server: $scope.server.name,
      type: 'realtime',
      kind: 'chrono',
      names: "db.*.createRecord,db.*.updateRecord,db.*.readRecord,db.*.deleteRecord"
    };

    var totalLoad = Metric.get(params);

    totalLoad.$promise.then(function (data) {
      console.log(data);
      data.result.forEach(function (e) {

      })
    });
  }


  var statsWatching = function () {
    $timeout(function () {
      initStats()
      statsWatching();
    }, 2000);
  }


  statsWatching();

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
