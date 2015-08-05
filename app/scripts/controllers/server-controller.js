/**
 * Created with JetBrains WebStorm.
 * User: erisa
 * Date: 02/09/13
 * Time: 18.41
 * To change this template use File | Settings | File Templates.
 */

var ctrl = angular.module('server.controller', []);
ctrl.controller("ServerController", ['$scope', '$routeParams', 'ServerApi', 'Database', 'ngTableParams', function ($scope, $routeParams, ServerApi, Database, ngTableParams) {

  $scope.active = $routeParams.tab || "conn";
  $scope.database = Database;
  $scope.tabs = ["conn", "config", "storage"];
  $scope.version = Database.getVersion();
  Database.setWiki("Server-Management.html")


  ServerApi.getServerInfo(function (data) {
    $scope.connections = data.connections;
    $scope.properties = data.properties;
    $scope.storages = data.storages;

    $scope.tableParams = new ngTableParams({
      page: 1,            // show first page
      count: 10          // count per page

    }, {
      total: $scope.connections.length, // length of data
      getData: function ($defer, params) {
//            use build-in angular filter
        var emtpy = !params.orderBy() || params.orderBy().length == 0;
        var orderedData = (params.sorting() && !emtpy) ?
          $filter('orderBy')($scope.connections, params.orderBy()) :
          $scope.connections;


        $defer.resolve(orderedData.slice((params.page() - 1) * params.count(), params.page() * params.count()));
      }
    });
  });


  $scope.getTemplate = function (tab) {
    return 'views/server/' + tab + '.html';
  }

  $scope.killConnection = function (n) {
    ServerApi.killConnection(n.connectionId, function () {
      var index = $scope.connections.indexOf(n);
      $scope.connections.splice(index, 1);
    });
  }
  $scope.interruptConnection = function (n) {
    ServerApi.interruptConnection(n.connectionId, function () {
      var index = $scope.connections.indexOf(n);
      $scope.connections.splice(index, 1);
    });
  }
}]);


ctrl.controller("ServerStatusController", ['$scope', '$rootScope', function ($scope, $rootScope) {

  $scope.isDown = false;
  $scope.serverclass = 'hide';
  $rootScope.$on('server:down', function () {
    $scope.isDown = true;
  })
  $rootScope.$on('server:up', function () {
    $scope.isDown = false;
  })
}]);

ctrl.controller('MultipleServerController', function ($scope, $rootScope, $location, $routeParams, $timeout, Cluster, Profiler, $q) {

  $scope.clustered = false;

  $scope.polling = true;
  $scope.agent = true;

  var singlePoll = function () {
    Profiler.realtime().then(function (data) {

      data.status = "ONLINE";
      data.name = "orientdb";
      $rootScope.$broadcast('server:updated', data);
    });
  }

  var calculateToRemove = function (tmpServers) {

    var toRemove = []
    tmpServers.forEach(function (s) {
      var found = false;
      $scope.servers.forEach(function (s1) {
        if (s1.name == s2.name) {
          found = true;
        }
      })
      if (!found) {
        toRemove.push(s);
      }
    })
    return toRemove;
  }
  var calculateToAdd = function (tmpServers) {

    var toAdd = []
    $scope.servers.forEach(function (s) {
      var found = false;
      tmpServers.forEach(function (s1) {
        if (s1.name == s2.name) {
          found = true;
        }
      })
      if (!found) {
        toAdd.push(s);
      }
    })
    return toAdd;
  }
  var containsServer = function (s, servers) {
    var found = false;
    servers.forEach(function (s1) {
      if (s.name == s1.name) {
        found = true;
      }
    })
    return found;
  }
  var multiplePoll = function () {
    Cluster.stats().then(function (data) {
      var keys = Object.keys(data.clusterStats);
      var tmpServers = []
      for (var i in keys) {
        var s = JSON.parse(data.clusterStats[keys[i]]);
        s.name = keys[i];
        s.status = "ONLINE"
        tmpServers.push(s);

        if (!containsServer(s, $scope.servers)) {
          $scope.servers.push(s);
        }
        $rootScope.$broadcast("server:updated", s);
      }
      $scope.servers.forEach(function (s) {
        if (!containsServer(s, tmpServers)) {
          $scope.servers.splice($scope.servers.indexOf(s), 1);
        }
      })

      $scope.serverClass = calculateSpan($scope.servers);
    }).catch(function (e) {

    });

  }

  var calculateSpan = function (servers) {

    if (servers.length == 1) {
      return 'col-md-12';
    }
    if (servers.length == 2) {
      return 'col-md-6';
    }
    if (servers.length > 2) {
      return 'col-md-3';
    }
    return 'col-md-12';
  }
  var initMonitoring = function () {
    Cluster.stats().then(function (data) {

      var keys = Object.keys(data.clusterStats);
      $scope.servers = [];


      for (var i in keys) {
        var s = JSON.parse(data.clusterStats[keys[i]]);
        s.name = keys[i];
        s.status = "ONLINE"
        $scope.servers.push(s);
      }
      $scope.serverClass = calculateSpan($scope.servers);
      statsWatching(multiplePoll);
    }).catch(function (err) {


      if (err.status == 400) {
        Profiler.realtime().then(function (data) {
          $scope.servers = [];
          data.status = "ONLINE"
          data.name = "orientdb";
          $scope.servers.push(data);

          $scope.serverClass = calculateSpan($scope.servers);
          statsWatching(singlePoll);

        }).catch(function (err) {
          if (err.status == 500) {
            $scope.servers = [];
            $scope.agent = false;
            $scope.servers.push({status: 'AGENT NOT FOUND'});
          }
        })
      } else if (err.status = 405) {
        $scope.servers = [];
        $scope.agent = false;
        $scope.servers.push({status: 'AGENT NOT FOUND'});
      }
    })
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
    }, 2000);
  }
  initMonitoring();
});
ctrl.controller('SingleChartServerController', function ($scope, $rootScope, $location, $routeParams, $timeout, Profiler, $q) {


})
ctrl.controller('SingleServerController', function ($scope, $rootScope, $location, $routeParams, $timeout, Profiler, $q) {


  $scope.links = {
    ee: "http://www.orientdb.com/orientdb-enterprise"
  }
  $scope.polling = true;
  $scope.operations = 0;

  $scope.connections = 0;

  $scope.requests = 0;

  $scope.latency = 0;

  $scope.currentIndex = 0;
  var lastOps = null;
  var lastReq = null;


  function initParamenters(realtime) {
    if (realtime) {
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

      var total = realtime['hookValues']['system.disk./.totalSpace'];
      var usable = realtime['hookValues']['system.disk./.usableSpace'];
      $scope.diskPercent = Math.floor((100 - (usable * 100) / total));
      $scope.anotherPercent = -45;
      $scope.diskOptions = {
        barColor: '#E67E22',
        scaleColor: false,
        lineWidth: 3,
        lineCap: 'butt'
      };
      $scope.tips = Object.keys(realtime['tips']).length;

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


      if (realtime['chronos']['server.network.requests']) {

        var req = realtime['chronos']['server.network.requests'].entries;
        if (lastReq != null) {
          $scope.requests = Math.abs(req - lastReq);
        }
        lastReq = req;
      }

      if (realtime['chronos']['distributed.node.latency'])
        $scope.latency = realtime['chronos']['distributed.node.latency'].average;
    }
  }

  function initStats() {


    if ($scope.server.status == 'ONLINE') {

      var realtime = $scope.server['realtime'];
      initParamenters(realtime);
    }

  }

  $rootScope.$on('server:updated', function (evt, s) {

    if (!s.name || s.name == $scope.server.name) {
      var realtime = s['realtime'];
      initParamenters(s.realtime);
    }
  });


  initStats();

});


ctrl.controller("ServerDashboardController", ['$scope', '$routeParams', 'Aside', 'ServerApi', 'ngTableParams', '$q', 'Notification','Database', function ($scope, $routeParams, Aside, ServerApi, ngTableParams, $q, Notification,Database) {

  Aside.show({
    scope: $scope,
    title: "",
    template: 'views/server/dashboardAside.html',
    show: true,
    absolute: false,
    small: true,
    sticky: true,
    cls: 'oaside-small'
  });


  $scope.version = Database.getVersion();

  $scope.dirtyProperties = [];
  ServerApi.getServerInfo(function (data) {
    $scope.connections = data.connections;
    $scope.properties = data.properties;
    $scope.storages = data.storages;

    $scope.globalProperties = data.globalProperties;

    if ($scope.globalProperties) {
      $scope.oldGlobal = $scope.globalProperties.filter(function (p) {
        return p.canChange;
      })
    }


  });


  $scope.menus = [
    {name: "stats", template: 'stats', icon: 'fa-bar-chart'},
    //{name: "cluster", template: 'distributed', icon: 'fa-sitemap'},
    {name: "connections", template: 'conn', icon: 'fa-plug'},
    {name: "configuration", template: 'config', icon: 'fa-cogs'},
    {name: "storage", template: 'storage', icon: 'fa-database'}
  ]
  if ($routeParams.tab) {


    $scope.menus.forEach(function (e) {
      if (e.name == $routeParams.tab) {
        $scope.active = e.template;
        $scope.current = e;
      }
    })
  }

  if (!$scope.active) {
    $scope.active = "stats";
    $scope.current = $scope.menus[0];
  }


  $scope.getTemplate = function (tab) {
    return 'views/server/stats/' + tab + '.html';
  }

  $scope.changeGlobal = function (prop) {
    if ($scope.dirtyProperties.indexOf(prop) == -1) {
      $scope.dirtyProperties.push(prop);
    }
  }
  $scope.saveGlobalProperties = function () {

    var promises = []
    $scope.dirtyProperties.forEach(function (p) {
      promises.push(ServerApi.changeConfiguration(p.key, p.value));
    })

    $q.all(promises).then(function (data) {
      Notification.push({content: data, autoHide: true});
    }).catch(function (data) {
      Notification.push({content: data, error: true, autoHide: true});
    })
  }

}]);

ctrl.controller('ServerConnectionController', function ($scope, $filter, ngTableParams) {


  $scope.init = false;

  $scope.$watch('connections', function () {
    if ($scope.connections) {
      $scope.tableParams = new ngTableParams({
        page: 1,            // show first page
        count: 10          // count per page

      }, {
        total: $scope.connections.length, // length of data
        getData: function ($defer, params) {
//            use build-in angular filter
          var emtpy = !params.orderBy() || params.orderBy().length == 0;

          var orderedData = $scope.query ?
            $filter('filter')($scope.connections, $scope.query) :
            $scope.connections;
          orderedData = (params.sorting() && !emtpy) ?
            $filter('orderBy')(orderedData, params.orderBy()) :
            orderedData;
          $defer.resolve(orderedData.slice((params.page() - 1) * params.count(), params.page() * params.count()));
          $scope.init = true;
        }
      });
      $scope.$watch("query", function (e) {
        if (e || $scope.init)
          $scope.tableParams.reload();
      });
    }
  })
})

ctrl.controller('ClusterController', function ($scope, Cluster, Notification) {


  Cluster.node().then(function (data) {
  }).catch(function (error) {
    Notification.push({content: error.data, error: true, autoHide: true});
  })
  Cluster.stats().then(function (data) {

    Object.keys(data.localNode.databases).forEach(function (db) {
      Cluster.database(db).then(function (data) {
        console.log(data);
      })
    });

  }).catch(function (error) {
    Notification.push({content: error.data, error: true, autoHide: true});
  })
})
