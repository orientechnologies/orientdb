var ee = angular.module('ee.controller', ['ee.services']);


ee.controller('GeneralMonitorController', function ($scope, $location, $routeParams, Cluster) {


  $scope.rid = $routeParams.server;


  $scope.tab = $routeParams.db;

  $scope.profilerOff = {content: "The Profiler for this server is Off. Just click the switch button above."}
  $scope.error = false;
  $scope.currentTab = 'overview';

  if ($scope.tab) {
    $scope.currentWarnings = true;
    $scope.currentTab = $scope.tab;
  }


  $scope.formatAddress = function (server) {
    if (server) {
      var address = ""
      var ports = " [";
      server.listeners.forEach(function (l, idx, arr) {
        if (idx == 0) {
          address += l.listen.split(":")[0];
        }
        ports += l.listen.split(":")[1];
        if (idx < arr.length - 1) {
          ports += ",";
        }
      });
      ports += "]";
      return address + ports;
    }
  }


  Cluster.node().then(function (data) {
    $scope.servers = data.members;
    $scope.server = $scope.servers[0];
  });

  //Monitor.getServers(function (data) {
  //  $scope.servers = data.result;
  //  if (!$scope.rid && $scope.servers.length > 0) {
  //    $scope.rid = $scope.servers[0]['@rid'];
  //    $scope.server = $scope.servers[0];
  //  } else if ($scope.servers.length > 0) {
  //    $scope.servers.forEach(function (server) {
  //      if ($scope.rid.replace('#', '') == server['@rid'].replace('#', '')) {
  //        $scope.server = server;
  //        return;
  //      }
  //    });
  //  }
  //  $scope.attached = $scope.server.attached;
  //});


  $scope.editorOptions = {
    lineWrapping: true,
    lineNumbers: true,
    readOnly: true,
    mode: 'xml'
  };


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
      server.attached = true;
      $scope.attached = server.attached;

      $scope.databases = server.databases;

      Cluster.configFile(server).then(function (data) {
        $scope.configuration = data;
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

    Cluster.backUp($scope.server, db);
    //Server.backUpDb($scope.server, db);
  }
  $scope.$watch('dbselected', function (data) {

    if (data) {
      $scope.getDbMetrics(data);
    }
  });

  $scope.$watch('databases', function (data) {

  });


});


ee.controller('SinglePollerController', function ($scope, $rootScope, $location, $routeParams, $timeout, Profiler, Cluster) {


  $scope.polling = true;

  var singlePoll = function () {

    Cluster.stats($scope.server.name).then(function (data) {
      data.name = $scope.server.name;
      $rootScope.$broadcast('server:updated', data);
    });
  }


  $rootScope.$on("$routeChangeStart", function (event, next, current) {
    $scope.polling = false;
  });
  var statsWatching = function (polling) {
    $timeout(function () {
      if ($scope.polling) {
        polling();
        statsWatching(polling);
      }
    }, POLLING);
  }

  statsWatching(singlePoll);
})


ee.controller('ClusterController', function ($scope, Cluster, Notification, $rootScope, $timeout) {


  $scope.polling = true;
  var clusterPolling = function () {
    Cluster.stats().then(function (data) {

      $scope.servers = data.members;

      $scope.clusterStats = data.clusterStats;
      //Object.keys(data.localNode.databases).forEach(function (db) {
      //  Cluster.database(db).then(function (data) {
      //    console.log(data);
      //  })
      //});

    }).catch(function (error) {
      Notification.push({content: error.data, error: true, autoHide: true});
    })
  }
  var statsWatching = function (polling) {
    $timeout(function () {
      if ($scope.polling) {
        polling();
        statsWatching(polling);
      }
    }, POLLING);
  }

  $rootScope.$on("$routeChangeStart", function (event, next, current) {
    $scope.polling = false;
  });

  statsWatching(clusterPolling)
})


ee.controller('ClusterOverviewController', function ($scope) {


  $scope.operations = 0;

  $scope.activeConnections = 0;

  $scope.requests = 0;

  $scope.latency = 0;

  $scope.cpu = 0;
  $scope.$watch('clusterStats', function (data) {
    if (data) {

      var keys = Object.keys(data);
      var cpu = 0;
      keys.forEach(function (val) {
        var realtime = data[val].realtime;
        var cpuN = realtime['hookValues']['process.runtime.cpu'];
        cpu += parseFloat(cpuN);
      })

      $scope.cpu = (cpu / keys.length).toFixed(2);
      console.log($scope.cpu);
    }
  })
});
