/**
 * Created with JetBrains WebStorm.
 * User: erisa
 * Date: 02/09/13
 * Time: 18.41
 * To change this template use File | Settings | File Templates.
 */

import {POLLING} from '../constants';

import '../views/server/dashboard.html';
import '../views/server/stats/stats.html';
import '../views/server/stats/general.html';
import '../views/server/general/overview.html';
import '../views/server/general/monitoring.html';
import '../views/server/general/metrics.html';
import '../views/server/general/conn.html';
import '../views/server/general/threads.html';
import '../views/widget/restartButton.html';


let ServerModule = angular.module('server.controller', []);
ServerModule.controller("ServerController", ['$scope', '$routeParams', 'ServerApi', 'Database', 'NgTableParams', function ($scope, $routeParams, ServerApi, Database, ngTableParams) {

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

}]);


ServerModule.controller("ServerStatusController", ['$scope', '$rootScope', function ($scope, $rootScope) {

  $scope.isDown = false;
  $scope.counter = 10;
  $scope.serverclass = 'hide';
  $rootScope.$on('server:down', function () {
    $scope.isDown = true;
  })
  $rootScope.$on('server:retry', function (evt, val) {
    $scope.counter = val;
  })
  $rootScope.$on('server:up', function () {
    $scope.isDown = false;
  })

  $scope.retry = function () {
    $rootScope.$broadcast("server:check");
  }
}]);

ServerModule.controller('MultipleServerController', function ($scope, $rootScope, $location, $routeParams, $timeout, Cluster, Profiler, $q, AgentService) {

  $scope.clustered = false;

  $scope.polling = true;
  $scope.agent = true;


  $scope.links = {
    ee: "http://www.orientdb.com/orientdb-enterprise"
  }
  $scope.agentActive = AgentService.active;


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
        var s = data.clusterStats[keys[i]];
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

      $scope.height = calculateChartHeight($scope.servers);
    }).catch(function (e) {

    });

  }

  var calculateChartHeight = function (servers) {
    if (servers.length == 1) {
      return 150;
    }
    if (servers.length == 2) {
      return 100;
    }
    if (servers.length > 2) {
      return 80;
    }
    return 150;
  }
  var calculateSpan = function (servers) {

    if (servers.length == 1) {
      return 'col-md-12';
    }
    if (servers.length == 2) {
      return 'col-md-6';
    }
    if (servers.length >= 3) {
      return 'col-md-4';
    }
    return 'col-md-12';
  }
  var initMonitoring = function () {

    if (!AgentService.active) {
      $scope.servers = [];
      $scope.agent = false;
      $scope.servers.push({status: 'AGENT NOT FOUND'});
    } else {
      Cluster.stats().then(function (data) {

        var keys = Object.keys(data.clusterStats);
        $scope.servers = [];


        for (var i in keys) {
          var s = data.clusterStats[keys[i]];
          s.name = keys[i];
          s.status = "ONLINE"
          $scope.servers.push(s);
        }
        $scope.serverClass = calculateSpan($scope.servers);
        multiplePoll();
        statsWatching(multiplePoll);
      }).catch(function (err) {


        if (err.status == 400) {
          Profiler.realtime().then(function (data) {
            $scope.servers = [];
            data.status = "ONLINE"
            data.name = "orientdb";
            $scope.servers.push(data);

            $scope.serverClass = calculateSpan($scope.servers);
            singlePoll();
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
  initMonitoring();
});
ServerModule.controller('SingleChartServerController', function ($scope, ChartHelper) {

  $scope.transactionHeaders = angular.copy(ChartHelper.serverStatsHeader);
})
ServerModule.controller('SingleServerController', function ($scope, $rootScope, $location, $routeParams, $timeout, Profiler, $q, AgentService) {


  $scope.links = {
    ee: "http://www.orientdb.com/orientdb-enterprise"
  }

  $scope.agentActive = AgentService.active;
  $scope.polling = true;
  $scope.operations = 0;

  $scope.connections = 0;

  $scope.requests = 0;

  $scope.latency = 0;

  $scope.currentIndex = 0;
  var lastOps = null;
  var lastReq = null;


  function initParamenters(realtime) {
    if (realtime && realtime['statistics']['process.runtime.maxMemory']) {

      // RAM

      var maxMemory = realtime['statistics']['process.runtime.maxMemory'].last;
      var totalMemory = realtime['statistics']['process.runtime.totalMemory'].last;
      var availableMemory = realtime['statistics']['process.runtime.availableMemory'].last;


      var used = totalMemory - availableMemory;

      $scope.usedRam = used;
      $scope.maxRam = maxMemory;
      $scope.ramPercent = Math.floor(((used * 100) / maxMemory));
      $scope.anotherPercent = -45;
      $scope.ramOptions = {
        barColor: '#E67E22',
        scaleColor: false,
        lineWidth: 3,
        lineCap: 'butt'
      };

      var total = realtime['sizes']['system.disk./.totalSpace'];
      var usable = realtime['sizes']['system.disk./.usableSpace'];
      $scope.diskPercent = Math.floor((100 - (usable * 100) / total));
      $scope.anotherPercent = -45;
      $scope.diskOptions = {
        barColor: '#E67E22',
        scaleColor: false,
        lineWidth: 3,
        lineCap: 'butt'
      };
      $scope.tips = Object.keys(realtime['tips']).length;


      // CPU
      var cpu = realtime['statistics']['process.runtime.cpu'].last;
      $scope.cpuPercent = parseFloat(cpu).toFixed(2);

      $scope.cpuOptions = {
        barColor: '#E67E22',
        scaleColor: false,
        lineWidth: 3,
        lineCap: 'butt'
      };

      // DISK CACHE

      var maxDiskCache = realtime['statistics']['process.runtime.diskCacheTotal'].last;
      var totalDiskCache = realtime['statistics']['process.runtime.diskCacheUsed'].last;


      $scope.maxDiskCacke = maxDiskCache;
      $scope.totalDiskCache = totalDiskCache;

      $scope.diskCache = Math.floor((totalDiskCache * 100) / maxDiskCache);


      $scope.connections = realtime['counters']['server.connections.actives'];

      var keys = Object.keys(realtime['counters']).filter(function (k) {
        return k.match(/db.*Record/g) != null;
      })
      var ops = 0;
      keys.forEach(function (k) {
        ops += realtime['counters'][k];
      });

      if (lastOps != null) {
        $scope.operations = Math.round(Math.abs(lastOps - ops) / (POLLING / 1000));
      }
      lastOps = ops;


      if (realtime['chronos']['server.network.requests']) {

        var req = realtime['chronos']['server.network.requests'].entries;
        if (lastReq != null) {
          $scope.requests = Math.round(Math.abs(req - lastReq) / (POLLING / 1000));
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


ServerModule.controller("ServerDashboardController", ['$scope', '$routeParams', 'Aside', 'ServerApi', 'NgTableParams', '$q', 'Notification', 'Database', function ($scope, $routeParams, Aside, ServerApi, ngTableParams, $q, Notification, Database) {

  $scope.version = Database.getVersion();

  $scope.dirtyProperties = [];


  $scope.menus = [
    {
      name: "stats", title: "Dashboard", template: 'stats', icon: 'fa-dashboard',
      wiki: "Studio-Dashboard.html"
    },
    {
      name: "general", title: "Servers Management", template: 'general', icon: 'fa-desktop',
      wiki: "Studio-Server-Management.html"
    },
    {
      name: "cluster", title: "Cluster Management", template: 'distributed', icon: 'fa-sitemap',
      wiki: "Studio-Cluster-Management.html"
    },
    {
      name: "profiler",
      title: "Query Profiler",
      template: 'profiler',
      icon: 'fa-rocket',
      wiki: "Studio-Query-Profiler.html"
    },
    {
      name: "backup",
      title: "Backup Management",
      template: 'backup',
      icon: 'fa-clock-o',
      wiki: "Studio-Backup-Management.html"
    },
    {name: "security", title: "Security", template: 'security', icon: 'fa-lock', wiki: "Security-Config.html"},
    {name: "teleporter", title: "Teleporter", template: 'teleporter', icon: 'fa-usb', wiki: "Studio-Teleporter.html"},
    {name: "plugins", title: "Plugins Management", template: 'plugins', icon: 'fa-plug'},
    {
      name: "alerts",
      title: "Alerts Management",
      template: 'events',
      icon: 'fa-bell',
      wiki: "Studio-Alert-Management.html"
    }
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

  $scope.getWiki = function (c) {
    return Database.resolveWiki(c.wiki);
  }

  $scope.getTemplate = function (tab) {
    return 'views/server/stats/' + tab + '.html';
  }

  $scope.changeGlobal = function (prop) {
    if ($scope.dirtyProperties.indexOf(prop) == -1) {
      $scope.dirtyProperties.push(prop);
    }
  }

  $scope.getType = function (val) {


    var type = typeof val;

    var formType = "text";
    switch (type) {
      case "boolean":
        formType = "checkbox";
        break;
      case "number":
        formType = "number";
        break;
      default :


    }

    return formType;
  }

  $scope.isNumber = function (val) {

  }
  $scope.isBoolean = function (val) {
    return (typeof val === "boolean");
  }
  $scope.isText = function (val) {

  }
  $scope.saveGlobalProperties = function () {

    var promises = []
    if ($scope.dirtyProperties.length > 0) {
      $scope.dirtyProperties.forEach(function (p) {
        promises.push(ServerApi.changeConfiguration(p.key, p.value));
      })

      $q.all(promises).then(function (data) {
        $scope.dirtyProperties = [];
        Notification.push({content: data, autoHide: true});
      }).catch(function (data) {
        Notification.push({content: data, error: true, autoHide: true});
      })
    }
  }

}]);

ServerModule.controller('ServerConnectionController', function ($scope, $filter, NgTableParams, Cluster, AgentService, ServerApi) {


  $scope.init = false;


  if (AgentService.active) {

    $scope.killConnection = function (n) {
      Cluster.killConnection($scope.server, n.connectionId).then(function () {
        var index = $scope.connections.indexOf(n);
        $scope.connections.splice(index, 1);
      });
    }
    $scope.interruptConnection = function (n) {
      Cluster.interruptConnection($scope.server, n.connectionId).then(function () {
        var index = $scope.connections.indexOf(n);
        $scope.connections.splice(index, 1);
      });
    }

    $scope.$watch('server', function (server) {

      Cluster.infoServer(server).then(function (info) {
        $scope.connections = info.connections;
      }).catch(function (err) {
        console.log(err);
      })
    });
  } else {
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
    ServerApi.getServerInfo(function (data) {
      $scope.connections = data.connections;
    })
  }


})


ServerModule.controller("LogsController", ['$scope', '$http', '$location', '$routeParams', 'CommandLogApi', 'Spinner', 'Cluster', 'AgentService', function ($scope, $http, $location, $routeParams, CommandLogApi, Spinner, Cluster, AgentService) {
  $scope.countPage = 1000;
  $scope.countPageOptions = [100, 500, 1000];
//  LOG_LEVEL.ERROR.ordinal() 4
//  LOG_LEVEL.CONFIG.ordinal() 7
//	LOG_LEVEL.DEBUG.ordinal() 0
//	LOG_LEVEL.INFO.ordinal() 1
//	LOG_LEVEL.WARN.ordinal() 3
  $scope.links = {
    ee: "http://www.orientdb.com/orientdb-enterprise"
  }

  $scope.agentActive = AgentService.active;
  $scope.types = ['CONFIG', 'DEBUG', 'ERROR', 'INFO', 'WARNI'];
  $scope.files = ['ALL_FILES', 'LAST'];
  $scope.selectedType = undefined;
  $scope.selectedFile = 'LAST';

  //Cluster.node().then(function (data) {
  //  $scope.servers = data.members;
  //  if ($scope.servers.length > 0) {
  //    $scope.server = $scope.servers[0]
  //  }
  //})

  $scope.$watch("server", function (data) {

    if (data) {
      CommandLogApi.getListFiles({server: $scope.server.name}, function (data) {


        if (data) {
          $scope.files = ['ALL_FILES', 'LAST'];
          $scope.selectedType = undefined;
          $scope.selectedFile = 'LAST';
          for (entry in data['files']) {
            $scope.files.push(data['files'][entry]['name']);
          }
          $scope.search();
        }
      });
    }
  });

  $scope.results = undefined;
  $scope.selectedSearch = '';
  $scope.getListFiles = function () {
    Spinner.start();
    CommandLogApi.getListFiles({server: $scope.server}, function (data) {

      if (data) {
        for (entry in data['files']) {
          $scope.files.push(data['files'][entry]['name']);
        }
      }
      Spinner.stopSpinner();
    }, function (error) {
      Spinner.stopSpinner();
    });
  }
  $scope.$watch("countPage", function (data) {
    if ($scope.resultTotal) {
      $scope.results = $scope.resultTotal.logs.slice(0, $scope.countPage);
      $scope.currentPage = 1;
      $scope.numberOfPage = new Array(Math.ceil($scope.resultTotal.logs.length / $scope.countPage));
    }
  });
  $scope.clear = function () {
    $scope.queries = new Array;
  }
  $scope.switchPage = function (index) {
    if (index != $scope.currentPage) {
      $scope.currentPage = index;
      $scope.results = $scope.resultTotal.logs.slice(
        (index - 1) * $scope.countPage,
        index * $scope.countPage
      );
    }
  }

  $scope.checkDateFrom = function () {
    if ($scope.selectedDateFrom == undefined || $scope.selectedDateFrom == '') {
      return true;
    }
    return false
  }
  $scope.checkHourFrom = function () {
    if ($scope.selectedHourFrom == undefined || $scope.selectedHourFrom == '') {
      return true;
    }
    return false
  }
  $scope.checkFile = function () {
    if ($scope.selectedFile == 'LAST') {
      return true;
    }
    return false;
  }
  $scope.previous = function () {
    if ($scope.currentPage > 1) {
      $scope.switchPage($scope.currentPage - 1);
    }
  }
  $scope.next = function () {

    if ($scope.currentPage < $scope.numberOfPage.length) {
      $scope.switchPage($scope.currentPage + 1);
    }
  }
  $scope.search = function () {
    Spinner.start();
    var typeofS = undefined;
    var filess = undefined;
    if ($scope.selectedFile == undefined || $scope.selectedFile == '') {
      return;
    }
    if ($scope.selectedFile == 'ALL_FILES') {
      typeofS = 'search';
    }
    else if ($scope.selectedFile == 'LAST') {
      typeofS = 'tail';
    }
    else {
      typeofS = 'file';
      filess = $scope.selectedFile;
    }


    if ($scope.server != undefined) {
      CommandLogApi.getLastLogs({
        server: $scope.server.name,
        file: filess,
        typeofSearch: typeofS,
        searchvalue: $scope.selectedSearch,
        logtype: $scope.selectedType,
        dateFrom: $scope.selectedDateFrom,
        dateTo: $scope.selectedDateTo
      }, function (data) {

        if (data) {

          $scope.resultTotal = data;
          if (data.logs) {
            $scope.results = data.logs.slice(0, $scope.countPage);
            $scope.currentPage = 1;
            $scope.numberOfPage = new Array(Math.ceil(data.logs.length / $scope.countPage));
          }
        }
        Spinner.stopSpinner();
      }, function (error) {
        Spinner.stopSpinner();
      });
    }
    else {
      Spinner.stopSpinner();
    }
  }
  $scope.selectType = function (selectedType) {
    $scope.selectedType = selectedType;
  }
  $scope.clearType = function () {
    $scope.selectedType = undefined;
  }
  $scope.clearSearch = function () {
    $scope.selectedSearch = undefined;
  }

}]);

export default ServerModule.name;
