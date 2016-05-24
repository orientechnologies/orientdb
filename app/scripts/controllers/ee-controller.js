var ee = angular.module('ee.controller', ['ee.services']);


ee.controller('GeneralMonitorController', function ($scope, $location, $routeParams, Cluster, AgentService) {


  $scope.rid = $routeParams.server;


  $scope.tab = $routeParams.db;


  $scope.links = {
    ee: "http://www.orientdb.com/orientdb-enterprise"
  }
  $scope.agentActive = AgentService.active;

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

  if (AgentService.active) {

    Cluster.node().then(function (data) {
      $scope.servers = data.members;
      $scope.server = $scope.servers[0];
    });
  }


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


ee.controller('SinglePollerController', function ($scope, $rootScope, $location, $routeParams, $timeout, Profiler, Cluster, AgentService) {


  $scope.polling = true;


  $scope.links = {
    ee: "http://www.orientdb.com/orientdb-enterprise"
  }
  $scope.agentActive = AgentService.active;

  var singlePoll = function () {

    if (AgentService.active) {
      Cluster.stats($scope.server.name).then(function (data) {
        data.name = $scope.server.name;
        $rootScope.$broadcast('server:updated', data);
      });
    } else {
      $scope.polling = false;
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

  statsWatching(singlePoll);
})


ee.controller('ClusterController', function ($scope, Cluster, Notification, $rootScope, $timeout, AgentService) {


  $scope.links = {
    ee: "http://www.orientdb.com/orientdb-enterprise"
  }
  $scope.polling = true;
  $scope.agentActive = AgentService.active;
  var clusterPolling = function () {

    if (AgentService.active) {
      Cluster.stats().then(function (data) {


        $scope.servers = data.members;


        $scope.clusterStats = data.clusterStats;
        $rootScope.$broadcast("server-list", $scope.servers);

      }).catch(function (error) {
        Notification.push({content: error.data, error: true, autoHide: true});
        $scope.polling = false;
      })
    } else {
      $scope.polling = false;
    }
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


ee.controller('ClusterOverviewController', function ($scope, $rootScope) {


  $scope.height = 100;

  $scope.status = 'ONLINE';
  $scope.operations = 0;

  $scope.activeConnections = 0;

  $scope.agent = true;

  $scope.requests = 0;

  $scope.latency = 0;

  $scope.cpu = 0;
  $scope.disk = 0;
  $scope.ram = 0;

  $scope.server = {name: "orientdb-cluster"};
  var lastRequest = null;
  var lastOps = null;

  $scope.$watch('clusterStats', function (data) {
    if (data) {


      var clusterCrud = {
        name: "orientdb-cluster",
        realtime: {chronos: {}}
      }
      var keys = Object.keys(data);
      var cpu = 0;
      var diskTotal = 0;
      var diskUsable = 0;
      var maxMemory = 0
      var totalMemory = 0;
      var availableMemory = 0;
      var connections = 0;
      var requests = 0;
      var latency = 0;
      var operations = 0;
      var maxDiskCache = 0;
      var totalDiskCache = 0;
      keys.forEach(function (val) {
        var realtime = data[val].realtime;
        // CPU
        var cpuN = realtime['statistics']['process.runtime.cpu'].last;
        cpu += parseFloat(cpuN);
        // DISK
        diskTotal += realtime['hookValues']['system.disk./.totalSpace'];
        diskUsable += realtime['hookValues']['system.disk./.usableSpace'];

        // RAM

        maxMemory += realtime['statistics']['process.runtime.maxMemory'].last;
        totalMemory += realtime['statistics']['process.runtime.totalMemory'].last;
        availableMemory += realtime['statistics']['process.runtime.availableMemory'].last;


        maxDiskCache += realtime['statistics']['process.runtime.diskCacheTotal'].last;
        totalDiskCache += realtime['statistics']['process.runtime.diskCacheUsed'].last;

        // CONNECTIONS

        connections += realtime['hookValues']['server.connections.actives'];


        if (realtime['chronos']['distributed.node.latency']) {
          latency += realtime['chronos']['distributed.node.latency'].average;
        }
        if (realtime['chronos']['server.network.requests']) {
          requests += realtime['chronos']['server.network.requests'].entries;
        }

        var keys = Object.keys(realtime['chronos']).filter(function (k) {
          return k.match(/db.*Record/g) != null;
        })
        var ops = 0;
        keys.forEach(function (k) {
          ops += realtime['chronos'][k].entries;

          if (!clusterCrud.realtime['chronos'][k]) {
            clusterCrud.realtime['chronos'][k] = {};
            clusterCrud.realtime['chronos'][k]['entries'] = realtime['chronos'][k].entries;
          } else {
            clusterCrud.realtime['chronos'][k]['entries'] += realtime['chronos'][k].entries;
          }
        });
        operations += ops;


      })

      $scope.cpu = (cpu / keys.length).toFixed(2);
      $scope.disk = Math.floor((100 - (diskUsable * 100) / diskTotal));

      $scope.latency = (latency / keys.length);

      var used = totalMemory - availableMemory;

      $scope.maxRam = maxMemory;
      $scope.usedRam = used;

      $scope.ram = Math.floor(((used * 100) / maxMemory));

      $scope.maxDiskCacke = maxDiskCache;
      $scope.totalDiskCache = totalDiskCache;

      $scope.diskCache = Math.floor((totalDiskCache * 100) / maxDiskCache);

      $scope.activeConnections = connections;


      if (lastRequest != null) {

        $scope.requests = Math.abs(requests - lastRequest);
      }
      lastRequest = requests;

      if (lastOps != null) {
        $scope.operations = Math.abs(lastOps - operations);
      }
      lastOps = operations;


      $rootScope.$broadcast('server:updated', clusterCrud);
    }
  })
});


ee.controller("ProfilerController", ['$scope', 'Profiler', 'Cluster', 'Spinner', 'Notification', 'CommandCache', 'Database', 'scroller', 'AgentService', function ($scope, Profiler, Cluster, Spinner, Notification, CommandCache, Database, scroller, AgentService) {


  $scope.strategies = ["INVALIDATE_ALL", "PER_CLUSTER"];


  $scope.links = {
    ee: "http://www.orientdb.com/orientdb-enterprise"
  }
  $scope.agentActive = AgentService.active;
  $scope.itemsByPage = 10;
  if (AgentService.active) {
    Cluster.node().then(function (data) {
      $scope.servers = data.members;
      $scope.server = $scope.servers[0];

    });
  }


  $scope.isLoading = false;
  $scope.refresh = function () {
    Spinner.start();
    var metricName = 'db.' + $scope.db + '.command.';
    $scope.isLoading = true;
    Profiler.profilerData({server: $scope.server.name, db: $scope.db}).then(function (data) {
      var profiling = $scope.flatten(data.realtime.chronos, metricName);


      $scope.profiles = profiling;
      $scope.isLoading = false;
      Spinner.stopSpinner();
    }).catch(function (error) {
      if (error.status == 405) {
        Notification.push({content: error.data, error: true, autoHide: true});
      } else {
        Notification.push({content: error.data, error: true, autoHide: true});
      }
      $scope.isLoading = false;
      Spinner.stopSpinner();
    })
  }

  $scope.reset = function () {

    Profiler.reset({server: $scope.server.name, db: $scope.db}).then(function (data) {
      $scope.refresh();
    })
  }
  $scope.$watch('server', function (server) {
    if (server) {
      if ($scope.server.databases.length > 0) {
        $scope.db = $scope.server.databases[0];
      }
    }
  });
  $scope.$watch('db', function (db) {

    if (db) {
      $scope.refresh();

      CommandCache.config({server: $scope.server.name, db: $scope.db}).then(function (data) {
        $scope.cache = data;
      });

      CommandCache.results({server: $scope.server.name, db: $scope.db}).then(function (data) {
        $scope.results = data.results;
      });
    }
  });

  $scope.changeEnable = function () {


    if ($scope.cache.enabled) {

      CommandCache.enable({server: $scope.server.name, db: $scope.db}).then(function () {

        Notification.push({content: "Command Cache enabled", autoHide: true});
      });
    } else {
      CommandCache.disable({server: $scope.server.name, db: $scope.db}).then(function () {
        Notification.push({content: "Command Cache disabled", autoHide: true});
      });
    }
  }

  $scope.saveConfig = function () {
    CommandCache.saveConfig({server: $scope.server.name, db: $scope.db, config: $scope.cache}).then(function () {
      Notification.push({content: "Command Cache configuration saved correctly", autoHide: true});
    });
  }

  $scope.purgeCache = function () {

    CommandCache.purge({server: $scope.server.name, db: $scope.db}).then(function () {
      Notification.push({content: "Command Cache purged", autoHide: true});
      CommandCache.results({server: $scope.server.name, db: $scope.db}).then(function (data) {
        $scope.results = data.results;
        $scope.resultsSet = null;
        $scope.headers = null;
      });
    });
  }

  $scope.fetchResults = function (q) {
    $scope.resultsSet = null;
    $scope.headers = null;
    CommandCache.queryResults({server: $scope.server.name, db: $scope.db, query: q.query}).then(function (data) {

      $scope.headers = Database.getPropertyTableFromResults(data.result);
      $scope.resultsSet = data.result;

      var someElement = angular.element(document.getElementById('results-id'));
      scroller.scrollToElement(someElement, 0, 2000);
    });
  }
  $scope.flatten = function (result, metricName) {
    var commands = new Array;
    Object.keys(result).forEach(function (e, i, a) {
      var obj = {};
      obj.name = e.substring(metricName.length, e.length);
      Object.keys(result[e]).forEach(function (ele, ide, arr) {
        obj[ele] = result[e][ele];
      });

      commands.push(obj);

    });
    return commands;
  }
  $scope.$watch('profiles', function (data) {

  })
}]);


ee.controller("AuditingController", ['$scope', 'Auditing', 'Cluster', 'Spinner', 'Notification', '$modal', 'ngTableParams', 'AgentService', 'SecurityService', function ($scope, Auditing, Cluster, Spinner, Notification, $modal, ngTableParams, AgentService, SecurityService) {


  $scope.enabled = false;
  $scope.clazz = "tabs-style-line";
  $scope.links = {
    ee: "http://www.orientdb.com/orientdb-enterprise"
  }
  $scope.agentActive = AgentService.active;

  $scope.active = 'log';
  $scope.logs = [];
  $scope.query = {
    limit: 100
  }

  if (AgentService.active) {
    Cluster.node().then(function (data) {
      $scope.servers = data.members;
      $scope.server = $scope.servers[0];

      if ($scope.server.databases.length > 0) {
        $scope.db = $scope.server.databases[0];

      }
    });

  }
  $scope.itemsByPage = 10;

  $scope.template = 'views/server/stats/auditing/log.html';

  var initConfig = function () {

    SecurityService.get().then(function (security) {
      try {
        $scope.auditingCfg = security.auditing;
        $scope.enabled = security.auditing.enabled
      } catch (e) {
      }
      if ($scope.enabled) {
        Auditing.getConfig({db: $scope.db}).then(function (data) {
          $scope.config = data;
          var cls = $scope.config.classes;
          $scope.classes = Object.keys(cls).filter(function (k) {
            return (k != "@type" && k != "@version")
          }).map(function (k) {

            var clazz = {
              name: k,
              polymorphic: cls[k].polymorphic
            }
            return clazz;
          })
          $scope.query.clazz = $scope.config.auditClassName;
          Spinner.start();
          Auditing.query({db: $scope.db, query: $scope.query}).then(function (data) {
            $scope.logs = data.result;
            Spinner.stopSpinner();
          }).catch(function (error) {
            Spinner.stopSpinner();
          })
        });
      }
    })

  }
  $scope.enableAuditing = function () {

    var config = angular.copy($scope.auditingCfg);
    config.enabled = !config.enabled;
    SecurityService.reload({"module": "auditing", "config": config}).then(function (c) {
      $scope.enabled = security.auditing.enabled
      initConfig();
    }).catch(function (e) {
      console.log(e);
    });
  }
  $scope.$watch("db", function (db) {
    if (db) {
      initConfig();
    }
  })
  $scope.resetFilter = function () {
    $scope.query = {
      limit: 100,
      clazz: $scope.auditClassName
    }
  }
  $scope.save = function () {

    Auditing.saveConfig({db: $scope.db}, $scope.config).then(function () {
      Notification.push({content: "Auditing configuration saved.", autoHide: true});
    }).catch(function (error) {

    })
  }

  $scope.filter = function () {
    Spinner.start();
    Auditing.query({db: $scope.db, query: $scope.query}).then(function (data) {

      $scope.logs = data.result;
      //$scope.tableParams.total($scope.logs.length);
      //$scope.tableParams.reload();
      Spinner.stopSpinner();
    }).catch(function (error) {
      Spinner.stopSpinner();
    })
  }
  $scope.$watch("active", function (val) {
    switch (val) {
      case "config":
        $scope.template = 'views/server/stats/auditing/config.html';
        break;
      case "log":
        $scope.template = 'views/server/stats/auditing/log.html';
        break;
    }
  })
  $scope.delete = function (k) {
    delete $scope.classes[k];
  }
  $scope.addCommand = function () {
    if (!$scope.config.commands) {
      $scope.config.commands = new Array;
    }
    $scope.config.commands.push({
      regex: "",
      message: ""
    })
  }
  $scope.deleteCommand = function (index) {
    $scope.config.commands.splice(index, 1);
  }
  $scope.addClass = function () {
    var modalScope = $scope.$new(true);
    modalScope.classes = Database.listClasses();
    var modalPromise = $modal({template: 'views/database/auditing/newClass.html', scope: modalScope, show: false});

    modalScope.save = function () {
      if (modalPromise.$scope.selectedClass) {
        var cfg = {
          "polymorphic": true,
          "onCreateEnabled": false,
          "onCreateMessage": "",
          "onReadEnabled": false,
          "onReadMessage": "",
          "onUpdateEnabled": false,
          "onUpdateMessage": "",
          "onDeleteEnabled": false,
          "onDeleteMessage": ""
        }
        $scope.config.classes[modalPromise.$scope.selectedClass.name] = cfg;

      }
    }
    modalPromise.$promise.then(modalPromise.show);
  }
}]);


ee.controller('PluginsController', function ($scope, Plugins, Cluster, Notification, AgentService) {


  $scope.links = {
    ee: "http://www.orientdb.com/orientdb-enterprise"
  }

  $scope.agentActive = AgentService.active;
  $scope.editorOptions = {
    lineWrapping: true,
    lineNumbers: true,
    mode: 'javascript'
  };

  $scope.customTemplate = {
    'mail': 'views/server/plugins/mail.html',
    'automaticBackup': 'views/server/plugins/automaticBackup.html'
  }
  $scope.clazz = 'tabs-style-linebox';
  $scope.dirty = false;
  $scope.selectPlugin = function (plugin) {
    $scope.selectedPlugin = plugin;
    $scope.currentEditingPlugin = angular.copy($scope.selectedPlugin);
  }


  $scope.pluginTemplate = function (plugin) {

    var pluginTpl = $scope.customTemplate[plugin.name];

    return pluginTpl || 'test';
  }

  $scope.$watch('server', function (server) {

    if (server) {
      Plugins.all(server.name).then(function (data) {
        $scope.plugins = data.plugins.filter(function (p) {
          return p.name != 'ee-events';
        });
        $scope.selectedPlugin = $scope.plugins[0];
        $scope.currentEditingPlugin = angular.copy($scope.selectedPlugin);
      })
    }
  })

  $scope.$on('context:changed', function (evt, context) {
    $scope.selectPlugin(context);
  })

  $scope.saveConfiguration = function () {


    Plugins.saveConfig($scope.server.name, $scope.selectedPlugin.name, $scope.currentEditingPlugin.configuration).then(function (data) {
      $scope.dirty = false;
      $scope.selectedPlugin.configuration = data;
      $scope.selectPlugin($scope.selectedPlugin);

      Notification.push({content: "Plugin configuration saved correctly.", autoHide: true});
    }).catch(function (error) {

      Notification.push({content: error.data, error: true, autoHide: true});
    });
  }

  $scope.applyAll = function () {
    Plugins.saveConfig('_all', $scope.selectedPlugin.name, $scope.currentEditingPlugin.configuration).then(function (data) {
      $scope.dirty = false;
      $scope.selectedPlugin.configuration = data;
      $scope.selectPlugin($scope.selectedPlugin);
      Notification.push({content: "Plugin configuration saved correctly in all Servers", autoHide: true});
    }).catch(function (error) {

      Notification.push({content: error.data, error: true, autoHide: true});
    });
  }
})

ee.controller('MailController', function ($scope, $modal, Database) {


  $scope.mailWiki = Database.resolveWiki("Mail-Plugin.html");
  ;
  $scope.removeProfile = function () {

    var idx = $scope.profiles.indexOf($scope.profile);

    $scope.profiles.splice(idx, 1);

    if ($scope.profiles.length > 0) {
      $scope.profile = $scope.profiles[0];
    } else {
      $scope.profile = null;
    }
  }

  $scope.addProfile = function () {
    var modalScope = $scope.$new(true);

    modalScope.newProfile = {name: ''};

    var modalPromise = $modal({template: 'views/server/plugins/newProfile.html', scope: modalScope, show: false});

    modalScope.createProfile = function () {
      $scope.profiles.push(modalPromise.$scope.newProfile);

      $scope.profile = modalPromise.$scope.newProfile;
      modalPromise.hide();
    }


    modalPromise.$promise.then(modalPromise.show);

  }
  $scope.$watch('currentEditingPlugin', function (data) {
    if (data && data.name == 'mail') {
      $scope.profiles = data.configuration.profiles;
      $scope.profile = $scope.profiles[0]

    } else {
      $scope.profiles = []
      $scope.profile = null;
    }
  })

});


ee.controller('AutomaticBackupController', function ($scope, $modal, Database) {


  $scope.wiki = Database.resolveWiki("Automatic-Backup.html");


  $scope.modes = ["FULL_BACKUP", "INCREMENTAL_BACKUP", "EXPORT"];

  $scope.$watch('currentEditingPlugin', function (data) {

    if (data && data.name == 'automaticBackup') {

      $scope.config = data.configuration;
    }
  })


});

ee.controller('EEDashboardController', function ($scope, $rootScope, $routeParams) {


  $rootScope.$on('servermgmt:open', function () {
    $scope.show = "ee-view-show";
  })
  $rootScope.$on('servermgmt:close', function () {
    $scope.show = "";
  })


  $scope.menus = [
    {name: "stats", title: "Dashboard", template: 'stats', icon: 'fa-dashboard'},
    {name: "general", title: "Servers Management", template: 'general', icon: 'fa-desktop'},
    {name: "cluster", title: "Cluster Management", template: 'distributed', icon: 'fa-sitemap'},
    {name: "backup", title: "Backup Management", template: 'backup', icon: 'fa-clock-o'},
    {name: "profiler", title: "Query Profiler", template: 'profiler', icon: 'fa-rocket'},
    {name: "security", title: "Security", template: 'security', icon: 'fa-lock'},
    {name: "auditing", title: "Auditing", template: 'auditing', icon: 'fa-headphones'},
    {name: "teleporter", title: "Teleporter", template: 'teleporter', icon: 'fa-usb'},
    {name: "events", title: "Events Management", template: 'events', icon: 'fa-bell'}

  ]


  $rootScope.$on("$routeChangeStart", function (event, next, current) {
    if (next.params.tab) {
      $scope.menus.forEach(function (e) {
        if (e.name == next.params.tab) {
          $scope.activeTab = e;
        }
      })
    } else {
      $scope.activeTab = $scope.menus[0];
    }
  });

})


ee.controller('DatabasesController', function ($scope, $rootScope, AgentService) {


  $scope.agentActive = AgentService.active;

  $scope.$watch("server", function (server) {

    if (server) {
      server.attached = true;
      $scope.attached = server.attached;

      $scope.databases = server.databases;

    }
  });

})

ee.controller("WarningsController", function ($scope, $rootScope, AgentService) {


  $scope.agentActive = AgentService.active;
  $rootScope.$on('server:updated', function (evt, data) {


    var keyTips = data.realtime.tips;
    var keys = Object.keys(keyTips)
    var tips = new Array;
    keys.forEach(function (k) {
      var o = {warning: k, count: keyTips[k].count, time: keyTips[k].time}
      tips.push(o);
    })

    $scope.tips = tips;
  });

});

ee.controller('ClusterDBController', function ($scope, $rootScope, $timeout) {


  $scope.clazz = 'tabs-style-linebox';
  $scope.icon = 'fa-database';

  $scope.databases = null;
  $scope.$on('context:changed', function (evt, context) {

    $timeout(function () {
      $scope.$broadcast('db-chosen', {name: context, servers: $scope.databases[context]});
    }, 500)

  })
  $scope.$on('server-list', function (evt, servers) {
    if (!$scope.databases) {
      $scope.databases = {};
      servers.forEach(function (s) {
        s.databases.forEach(function (db) {
          if (!$scope.databases[db]) {
            $scope.databases[db] = [];
          }
          $scope.databases[db].push(s);
        })
      })
    }
  })

})

ee.controller('ClusterSingleDBController', function ($scope, Cluster, Notification) {


  $scope.roles = ["master", "replica"];


  $scope.quorums = ['majority'];


  $scope.$on('db-chosen', function (evt, db) {

    $scope.servers = angular.copy($scope.servers);

    $scope.servers.forEach(function (el, idx, arr) {
      $scope.quorums.push((idx + 1).toString());
    })
    Cluster.database(db.name).then(function (data) {
      $scope.config = data;
      $scope.name = db.name;


      $scope.calculatedRoles = {};
      if ($scope.config.servers) {
        Object.keys($scope.config.servers).forEach(function (k) {
          if (k === "*") {
            $scope.servers.forEach(function (s) {
              $scope.calculatedRoles[s.name] = $scope.config.servers[k];
            });
          } else {
            $scope.calculatedRoles[k] = $scope.config.servers[k];
          }
        })
      }
    })


    $scope.isClusterInNode = function (cluster, node) {
      var tmp = $scope.config.clusters[cluster];
      if (!tmp.servers)return false;
      return tmp.servers.indexOf(node) != -1;
    }

    $scope.getOwnership = function (cluster, node) {
      var tmp = $scope.config.clusters[cluster];
      if (!tmp.servers)return "";
      return tmp.servers.indexOf(node) == 0 ? "X" : "o";
    }

  })

  $scope.saveConfig = function () {

    Object.keys($scope.calculatedRoles).forEach(function (k) {
      var r = $scope.calculatedRoles[k];
      var oldRole = $scope.config.servers[k];
      if (oldRole) {
        delete $scope.config.servers[k];
      }
      if ($scope.config.servers["*"] != r) {
        $scope.config.servers[k] = r;
      }
    })

    Cluster.saveDBConfig({name: $scope.name, config: $scope.config}).then(function () {
      Notification.push({content: "Distributed Configuration correctly saved.", autoHide: true});
    }).catch(function (err) {
      Notification.push({content: err.data, error: true, autoHide: true});
    })

  }
})

ee.controller('EventsController', function ($scope, Plugins, $modal, Cluster, Profiler, Notification, AgentService) {

  var PNAME = "ee-events";


  $scope.links = {
    ee: "http://www.orientdb.com/orientdb-enterprise"
  }
  $scope.agentActive = AgentService.active;

  $scope.alertValues = [">=", "<="];

  $scope.parameters = ["value", "entries", "min", "max", "average", "total"];

  $scope.levels = ['OFFLINE', 'ONLINE'];

  $scope.eventWhen = [{title: "Metric", name: "MetricWhen"}, {title: "Status", name: 'LogWhen'}];

  $scope.eventWhat = [{title: "Mail", name: 'MailWhat'}, {title: "Http Request", name: 'HttpWhat'}];


  if (AgentService.active) {
    Plugins.one({plugin: 'mail'}).then(function (plugin) {
      $scope.profiles = plugin.profiles;
    });

    Profiler.metadata().then(function (data) {
      $scope.metadata = data.metadata;
    });
    Cluster.node().then(function (data) {
      $scope.servers = data.members;
      Plugins.one({plugin: PNAME}).then(function (plugin) {
        $scope.config = plugin;
        $scope.events = plugin.events;
      });
    });
  }

  $scope.addEvent = function () {
    if (!$scope.events) {
      $scope.events = [];
    }
    $scope.events.push({name: 'New Event', when: {name: $scope.eventWhen[0]}, what: {name: $scope.eventWhat[0]}});
  }
  $scope.dropEvent = function (e) {
    var idx = $scope.events.indexOf(e);
    $scope.events.splice(idx, 1);
  }

  $scope.changeWhen = function (e) {
    var name = e.when.name;
    e.when = {};
    e.when.name = name;
  }

  $scope.changeWhat = function (e) {
    var name = e.what.name;
    e.what = {};
    e.what.name = name;
  }

  $scope.configureWhen = function (when) {

    var modalScope = $scope.$new(true);


    modalScope.eventWhen = when;
    modalScope.levels = $scope.levels;
    modalScope.servers = $scope.servers;
    modalScope.metadata = $scope.metadata;
    modalScope.parameters = $scope.parameters;
    modalScope.alertValues = $scope.alertValues;

    if (when.name === 'MetricWhen') {

      modalScope.$watch('eventWhen.type', function (data, old) {

        if (data) {
          var type = modalScope.metadata[data].type;
          if (type === 'CHRONO' || type === 'STAT') {
            modalScope.parameters = ["entries", "min", "max", "average", "total"];
          } else {
            modalScope.parameters = ["value"];
          }
          if (modalScope.eventWhen.parameter) {

            if (!old || old != data) {
              modalScope.eventWhen.parameter = null;
            }
          }
        }
      })
    }
    var modalPromise = $modal({
      template: 'views/server/distributed/events/' + when.name.toLowerCase().trim() + '.html',
      scope: modalScope
    });

    modalPromise.$promise.then(modalPromise.show);
  }
  $scope.configureWhat = function (what) {

    var modalScope = $scope.$new(true);

    modalScope.eventWhat = what;
    modalScope.profiles = $scope.profiles;
    var modalPromise = $modal({
      template: 'views/server/distributed/events/' + what.name.toLowerCase().trim() + '.html',
      scope: modalScope
    });
    modalPromise.$promise.then(modalPromise.show);
  }
  $scope.saveConfig = function () {
    Plugins.saveConfig("_all", PNAME, $scope.config).then(function (data) {
      Notification.push({content: "Events configuration saved correctly", autoHide: true});
    })
  }
});

ee.controller('MetricsController', function ($scope, Cluster, AgentService) {

  $scope.clazz = 'tabs-style-linebox';

  $scope.agentActive = AgentService.active;

  $scope.$watch('server', function (server) {

    Cluster.stats(server.name).then(function (data) {

      $scope.chronos = Object.keys(data.realtime.chronos).filter(function (k) {
        return k.match(/db.*command/g) == null;
      }).map(function (k) {
        var obj = {};
        angular.copy(data.realtime.chronos[k], obj);
        obj.name = k;
        return obj
      });


      $scope.stats = Object.keys(data.realtime.statistics).map(function (k) {
        var obj = {};
        angular.copy(data.realtime.statistics[k], obj);
        obj.name = k;
        return obj
      });

      $scope.counters = Object.keys(data.realtime.counters).map(function (k) {
        var obj = {};
        obj.name = k;
        obj.value = data.realtime.counters[k];
        return obj
      });
      $scope.hookValues = Object.keys(data.realtime.hookValues).map(function (k) {
        var obj = {};
        obj.name = k;
        obj.value = data.realtime.hookValues[k];
        return obj
      });
    })
  })
});

ee.controller('TeleporterController', function ($scope, Teleporter, $timeout, Notification, AgentService) {


  $scope.links = {
    ee: "http://www.orientdb.com/orientdb-enterprise"
  }
  $scope.agentActive = AgentService.active;

  $scope.editorOptions = {
    lineWrapping: true,
    lineNumbers: true,
    viewportMargin: 20,

    onLoad: function (cm) {
      $scope.cm = cm;
      cm.setSize("100%", 400);
    }
  };

  $scope.levels = [{"0": "NO"}, {"1": "DEBUG"}, {"2": "INFO"}, {"3": "WARNING"}, {"4": "ERROR"}];
  $scope.strategies = ["naive", "naive-aggregate"];
  $scope.nameResolvers = ["original", "java"];
  $scope.mappers = ['basicDBMapper', 'hibernate']
  $scope.defaultConfig = {
    "driver": "PostgreSQL",
    "jurl": "jdbc:postgresql://<HOST>:<PORT>/<DB>",
    "username": "",
    "password": "",
    "outDbUrl": "",
    "strategy": "naive",
    "mapper": "basicDBMapper",
    "xmlPath": "",
    "nameResolver": "original",
    "level": "2",
    "includes": [],
    "excludes": []
  }


  $scope.config = angular.copy($scope.defaultConfig);

  $scope.includedClasses = [];
  $scope.excludedClasses = [];

  $scope.changeDriver = function () {

    $scope.config.jurl = $scope.drivers[$scope.config.driver].format;
  }
  Teleporter.drivers({}).then(function (data) {
    $scope.drivers = data;
  })
  $scope.finished = false;
  $scope.running = true;
  $scope.launch = function () {
    $scope.config.includes = $scope.includedClasses.map(function (c) {
      return c.text;
    })
    $scope.config.excludes = $scope.excludedClasses.map(function (c) {
      return c.text;
    })
    Teleporter.launch({config: $scope.config}).then(function (data) {
      $scope.running = true;
      status();
    });
  }

  $scope.testConnection = function () {
    $scope.config.includes = $scope.includedClasses.map(function (c) {
      return c.text;
    })
    $scope.config.excludes = $scope.excludedClasses.map(function (c) {
      return c.text;
    })
    Teleporter.test({config: $scope.config}).then(function (data) {
      Notification.push({content: "Connection is alive", autoHide: true});
    }).catch(function (error) {
      Notification.push({content: error.data, error: true, autoHide: true});
    });
  }


  var status = function () {

    if ($scope.running) {
      Teleporter.status({}).then(function (data) {
        $scope.status = data;
        if (data.jobs.length > 0) {


          if ($scope.job && $scope.job.log) {
            $scope.job.status = data.jobs[0].status;
            var pos = CodeMirror.Pos($scope.cm.lastLine());
            var log = data.jobs[0].log.replace($scope.job.log, "");
            $scope.cm.replaceRange(log, pos);
          } else {
            $scope.job = data.jobs[0];
          }

          $timeout(function () {
            $scope.cm.scrollTo(0, $scope.cm.getScrollInfo().height);
          })
        } else {
          if ($scope.job) {
            $scope.finished = true;
          }
          $scope.running = false;
        }
        $timeout(status, 3000);
      })
    }
  }

  status();

});


ee.controller("HttpWhatController", function ($scope) {
  $scope.methods = ["GET", "POST"];


  $scope.addHeader = function () {
    if (!$scope.eventWhat.headers) {
      $scope.eventWhat.headers = []
    }
    $scope.eventWhat.headers.push({name: "", value: ""});
  }
  $scope.removeHeader = function (h) {
    var idx = $scope.eventWhat.headers.indexOf(h);
    $scope.eventWhat.headers.splice(idx, 1);
  }
  $scope.checkMethod = function () {

    if ($scope.eventWhat['method'] == 'POST') {

      return false
    }
    else {
      $scope.eventWhat['body'] = undefined;
      return true
    }
  }
});

ee.controller("GlobalConfiController", function ($scope, ServerApi) {


  ServerApi.getServerInfo(function (data) {
    $scope.properties = data.properties;
    $scope.storages = data.storages;

    $scope.globalProperties = data.globalProperties;

    if ($scope.globalProperties) {
      $scope.oldGlobal = $scope.globalProperties.filter(function (p) {
        return p.canChange;
      })
    }

  });
});


ee.controller("BackupConfigController", function ($scope, AgentService, $rootScope, $timeout, Cluster, BackupService) {


  $scope.agentActive = AgentService.active;


  $scope.clazz = 'tabs-style-linebox';
  $scope.icon = 'fa-database';

  $scope.databases = null;


  $scope.$watch("database", function (db) {
    if (db) {
      $scope.$broadcast('db-chosen', {name: db.dbName, backup: db});
    }
  })

  if (AgentService.active) {
    Cluster.node().then(function (data) {
      $scope.servers = data.members;
      $scope.server = $scope.servers[0];


      $scope.databases = {};


      BackupService.get().then(function (d) {
        d.backups.forEach(function (b, idx, arr) {
          if (idx == 0) {
            $scope.database = b;
          }
          $scope.databases[b.dbName] = b;
        });

        $scope.server.databases.forEach(function (db) {
          if (!$scope.databases[db]) {
            $scope.databases[db] = {dbName: db};
          }
        });


      })

    });
  }
});

/**
 *  Single Backup Controller
 */
ee.controller("SingleBackupController", function ($scope, BackupService, Notification, $modal) {

  $scope.eventsType = [
    {name: "Scheduled", type: "BACKUP_SCHEDULED", clazz: 'log-scheduled-icon'},
    {name: "Finished", type: "BACKUP_FINISHED", clazz: 'log-finished-icon'},
    {name: "Restore Finished", type: "RESTORE_FINISHED", clazz: 'log-restore-finished-icon'},
    {name: "Error", type: "BACKUP_ERROR", clazz: 'log-error-icon'},
    {name: "Started", type: "BACKUP_STARTED", clazz: 'log-started-icon'},
    {name: "Restore Started", type: "RESTORE_STARTED", clazz: 'log-restore-started-icon'}
  ]
  $scope.selectedEvents = ["BACKUP_FINISHED", "BACKUP_ERROR", "RESTORE_FINISHED"]
  $scope.mode = "1";
  $scope.modes = {"1": "Incremental Backup", "2": "Full Backup", "3": "Full + Incremental Backup"};


  // CRON Expression

  $scope.incremental_cron = $('#incremental_cron').cron({
    onChange: function () {
      if ($scope.backup && $scope.backup.modes["INCREMENTAL_BACKUP"]) {
        $scope.backup.modes["INCREMENTAL_BACKUP"].when = $(this).cron("value");
      }
    }
  });
  $scope.full_cron = $('#full_cron').cron({
    onChange: function () {
      if ($scope.backup && $scope.backup.modes["FULL_BACKUP"]) {
        $scope.backup.modes["FULL_BACKUP"].when = $(this).cron("value");
      }
    }
  });


  $scope.onEventClick = function (t) {

    var idx = $scope.selectedEvents.indexOf(t.type);
    if (idx == -1) {
      $scope.selectedEvents.push(t.type);
    } else {
      $scope.selectedEvents.splice(idx, 1);
    }

    $('#calendar').fullCalendar('removeEvents');


    $('#calendar').fullCalendar('addEventSource', formatLogs($scope.currentUnitLogs))

  }

  function formatLogs(logs) {
    return logs.filter(function (e) {
      return $scope.selectedEvents.indexOf(e.op) != -1;
    }).map(function (e, idx, arr) {
      var date = new Date(e.timestamp);
      return {
        id: idx,
        title: $scope.info(e),
        _source: e,
        _template: '/views/server/backup/' + e.op.toLowerCase() + '.html',
        start: date,
        end: date,
        className: $scope.clazz(e)

      }
    })
  }


  $scope.getClazz = function (t) {
    return ($scope.selectedEvents.indexOf(t.type) == -1 ? "fa-circle-thin " : "fa-circle ") + t.clazz;
  }


  $scope.$on('db-chosen', function (evt, db) {

    $scope.backup = db.backup;

    var incr = undefined;
    var full = undefined;
    if ($scope.backup.modes) {
      if ($scope.backup.modes["INCREMENTAL_BACKUP"] && $scope.backup.modes["FULL_BACKUP"]) {
        $scope.mode = "3";
        incr = $scope.backup.modes["INCREMENTAL_BACKUP"].when;
        full = $scope.backup.modes["FULL_BACKUP"].when;
      } else if ($scope.backup.modes["FULL_BACKUP"]) {
        $scope.mode = "2";
        full = $scope.backup.modes["FULL_BACKUP"].when;
      } else if ($scope.backup.modes["INCREMENTAL_BACKUP"]) {
        $scope.mode = "1";
        incr = $scope.backup.modes["INCREMENTAL_BACKUP"].when;
      }
    }


    $scope.incremental_cron.cron('value', incr);
    $scope.full_cron.cron('value', full);
    function initCalendar(data) {
      $('#calendar').fullCalendar({
        header: {
          left: 'prev,next today',
          center: 'title',
          right: 'month,agendaWeek,agendaDay'
        },
        eventClick: function (calEvent, jsEvent, view) {

          var modalScope = $scope.$new(true);
          modalScope.event = calEvent;
          var modalPromise = $modal({template: 'views/server/backup/modalBackup.html', scope: modalScope, show: false});

          modalScope.restoreBackup = function (evt) {
            $scope.restore(evt._source);
          }

          modalScope.removeBackup = function (evt) {
            $scope.removeBkp(evt._source);
          }
          modalPromise.$promise.then(modalPromise.show);
        },
        defaultView: 'agendaWeek',
        editable: true,
        events: formatLogs(data.logs)
      })
    }

    if (!$scope.backup.uuid) {
      $scope.backup.dbName = db.name;
      $scope.backup.modes = {};
    } else {
      BackupService.logs($scope.backup.uuid).then(function (data) {
        $scope.logs = data.logs;
        if ($scope.logs.length > 0) {
          $scope.currentUnit = $scope.logs[0];
          BackupService.unitLogs($scope.backup.uuid, $scope.currentUnit.unitId).then(function (data) {
            $scope.currentUnitLogs = data.logs;
            initCalendar(data);
          })
        }
      });
    }

    $scope.removeBkp = function (evt) {

      var modalScope = $scope.$new(true);
      modalScope.restored = {unitId: evt.unitId};

      modalScope.onRemove = function (obj, callback) {


        BackupService.remove($scope.backup.uuid, obj).then(function (data) {
          callback();
          Notification.push({content: "Bakcup files removed", autoHide: true});
        }).catch(function (err) {
          callback();
          Notification.push({content: err.data, error: true, autoHide: true});
        })
      }
      BackupService.unitLogs($scope.backup.uuid, evt.unitId, {op: evt.op}).then(function (data) {
        modalScope.unitLogs = data.logs;
        modalScope.restored.log = evt;
        var modalPromise = $modal({template: 'views/server/backup/remove.html', scope: modalScope, show: false});
        modalPromise.$promise.then(modalPromise.show);
      })
    }
    $scope.restore = function (evt) {
      var modalScope = $scope.$new(true);
      modalScope.restored = {unitId: evt.unitId};
      modalScope.onRestore = function (obj, callback) {

        BackupService.restore($scope.backup.uuid, obj).then(function (data) {

          callback();
          Notification.push({content: "Restore procedure in progress into database " + obj.target, autoHide: true});
        }).catch(function (err) {
          callback();
          Notification.push({content: err.data, error: true, autoHide: true});
        })
      }


      BackupService.unitLogs($scope.backup.uuid, evt.unitId, {op: evt.op}).then(function (data) {
        modalScope.unitLogs = data.logs;
        modalScope.restored.log = evt;

        var modalPromise = $modal({template: 'views/server/backup/restore.html', scope: modalScope, show: false});
        modalPromise.$promise.then(modalPromise.show);
      })

    }


    var modeToString = function (mode) {

      switch (mode) {
        case "INCREMENTAL_BACKUP":
          return "Incremental backup";
        case "FULL_BACKUP":
          return "Full backup";
      }
    }

    $scope.clazz = function (event) {
      var clazz = "basic-log";

      switch (event.op) {
        case "BACKUP_FINISHED":
          clazz += " log-finished";
          break;
        case "BACKUP_SCHEDULED":
          clazz += " log-scheduled";
          break;
        case "BACKUP_STARTED":
          clazz += " log-started";
          break;
        case "BACKUP_ERROR":
          clazz += " log-error";
          break;
        case "RESTORE_FINISHED":
          clazz += " log-restore-finished";
          break;
      }
      return clazz;
    }
    $scope.info = function (event) {
      var info = modeToString(event.mode);
      switch (event.op) {
        case "BACKUP_FINISHED":
          info += " executed";
          break;
        case "BACKUP_ERROR":

          info += " error";
          break;
        case "BACKUP_SCHEDULED":
          info += " scheduled.";
          break;
        case "BACKUP_STARTED":
          info += " started";
          break;
      }
      return info;
    }
    $scope.$watch("mode", function (m) {
      if (m) {
        switch (m) {
          case "1":
            if (!$scope.backup.modes["INCREMENTAL_BACKUP"]) {
              $scope.backup.modes["INCREMENTAL_BACKUP"] = {when: "0 0/1 * * * ?"}
            }
            delete $scope.backup.modes["FULL_BACKUP"]
            break;
          case "2":
            if (!$scope.backup.modes["FULL_BACKUP"]) {
              $scope.backup.modes["FULL_BACKUP"] = {when: "0 0/1 * * * ?"}
            }
            delete $scope.backup.modes["INCREMENTAL_BACKUP"];
            break;
          case "3":
            if (!$scope.backup.modes["FULL_BACKUP"]) {
              $scope.backup.modes["FULL_BACKUP"] = {when: "0 0/1 * * * ?"}
            }
            if (!$scope.backup.modes["INCREMENTAL_BACKUP"]) {
              $scope.backup.modes["INCREMENTAL_BACKUP"] = {when: "0 0/1 * * * ?"}
            }
            break;
        }

      }
    })


  })


  $scope.save = function () {
    BackupService.save($scope.backup).then(function (data) {
      Notification.push({content: "Backup saved", autoHide: true});
    }).catch(function (err) {
      Notification.push({content: err.data, error: true, autoHide: true});
    });
  }
});


ee.controller("RestoreController", function ($scope) {


})
/**
 * Security Controller Auditing + Authenticator
 */
ee.controller("ServerSecurityController", function ($scope, AgentService) {


  $scope.agentActive = AgentService.active;
  //$scope.icon = 'fa-database';
  $scope.securityTabs = [
    {"name": "Auditing", "template": "views/server/stats/auditing.html"},
    {"name": "Kerberos", "template": "views/server/stats/kerberos.html"}
  ]


});
