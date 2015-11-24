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
      $rootScope.$broadcast("server-list", $scope.servers);

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
      keys.forEach(function (val) {
        var realtime = data[val].realtime;
        // CPU
        var cpuN = realtime['hookValues']['process.runtime.cpu'];
        cpu += parseFloat(cpuN);
        // DISK
        diskTotal += realtime['hookValues']['system.disk./.totalSpace'];
        diskUsable += realtime['hookValues']['system.disk./.usableSpace'];

        // RAM

        maxMemory += realtime['hookValues']['process.runtime.maxMemory'];
        totalMemory += realtime['hookValues']['process.runtime.totalMemory'];
        availableMemory += realtime['hookValues']['process.runtime.availableMemory'];

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

      $scope.ram = Math.floor(((used * 100) / maxMemory));

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


ee.controller("ProfilerController", ['$scope', 'Profiler', 'Cluster', 'Spinner', 'Notification', 'CommandCache', function ($scope, Profiler, Cluster, Spinner, Notification, CommandCache) {


  $scope.strategies = ["INVALIDATE_ALL", "PER_CLUSTER"];

  Cluster.node().then(function (data) {
    $scope.servers = data.members;
    $scope.server = $scope.servers[0];

  });

  $scope.itemsByPage = 4;
  $scope.profiles = []

  $scope.refresh = function () {
    Spinner.start();
    var metricName = 'db.' + $scope.db + '.command.';
    Profiler.profilerData({server: $scope.server.name, db: $scope.db}).then(function (data) {
      var profiling = $scope.flatten(data.realtime.chronos, metricName);
      $scope.profiles = profiling;
      $scope.safeCopy = angular.copy(profiling);
      Spinner.stopSpinner();
    }).catch(function (error) {
      if (error.status == 405) {
        Notification.push({content: error.data, error: true, autoHide: true});
      } else {
        Notification.push({content: error.data, error: true, autoHide: true});
      }
      Spinner.stopSpinner();
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
    }
  });

  $scope.changeEnable = function () {


    if ($scope.cache.enabled) {

      CommandCache.enable({server: $scope.server.name, db: $scope.db}).then(function () {

      });
    } else {
      CommandCache.disable({server: $scope.server.name, db: $scope.db}).then(function () {

      });
    }
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


ee.controller("AuditingController", ['$scope', 'Auditing', 'Cluster', 'Spinner', 'Notification', '$modal', 'ngTableParams', function ($scope, Auditing, Cluster, Spinner, Notification, $modal, ngTableParams) {


  $scope.active = 'log';

  $scope.query = {
    limit: 100
  }

  Cluster.node().then(function (data) {
    $scope.servers = data.members;
    $scope.server = $scope.servers[0];

    if ($scope.server.databases.length > 0) {
      $scope.db = $scope.server.databases[0];

      initConfig();
    }
  });

  $scope.template = 'views/database/auditing/log.html';

  var initConfig = function () {
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
      Auditing.query({db: $scope.db}, $scope.query).then(function (data) {
        $scope.logs = data.result;

        $scope.tableParams = new ngTableParams({
          page: 1,            // show first page
          count: 10          // count per page

        }, {
          total: $scope.logs.length, // length of data
          getData: function ($defer, params) {
//            use build-in angular filter
            var emtpy = !params.orderBy() || params.orderBy().length == 0;
            var orderedData = (params.sorting() && !emtpy) ?
              $filter('orderBy')($scope.logs, params.orderBy()) :
              $scope.logs;
            $defer.resolve(orderedData.slice((params.page() - 1) * params.count(), params.page() * params.count()));
          }
        });
        Spinner.stopSpinner();
      }).catch(function (error) {
        Spinner.stopSpinner();
      })
    });
  }

  $scope.resetFilter = function () {
    $scope.query = {
      limit: 100,
      clazz: $scope.config.auditClassName
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
    Auditing.query(Database.getName(), $scope.query).then(function (data) {
      $scope.logs = data.result;
      $scope.tableParams.total($scope.logs.length);
      $scope.tableParams.reload();
      Spinner.stopSpinner();
    }).catch(function (error) {
      Spinner.stopSpinner();
    })
  }
  $scope.$watch("active", function (val) {
    switch (val) {
      case "config":
        $scope.template = 'views/database/auditing/config.html';
        break;
      case "log":
        $scope.template = 'views/database/auditing/log.html';
        break;
    }
  })
  $scope.delete = function (k) {
    delete $scope.config.classes[k];
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


ee.controller('PluginsController', function ($scope, Plugins, Cluster, Notification) {

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

ee.controller('EEDashboardController', function ($scope, $rootScope) {


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
    {name: "profiler", title: "Query Profiler", template: 'profiler', icon: 'fa-rocket'},
    {name: "auditing", title: "Auditing", template: 'auditing', icon: 'fa-headphones'},
    {name: "events", title: "Events Management", template: 'events', icon: 'fa-bell'},
    {name: "configuration", title: "Settings", template: 'config', icon: 'fa-cogs'},
    {name: "storage", title: "Storages", template: 'storage', icon: 'fa-database'}
  ]
})


ee.controller('DatabasesController', function ($scope, $rootScope) {


  $scope.$watch("server", function (server) {

    if (server) {
      server.attached = true;
      $scope.attached = server.attached;

      $scope.databases = server.databases;

    }
  });

})

ee.controller("WarningsController", function ($scope, $rootScope) {


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

ee.controller('ClusterDBController', function ($scope, $rootScope) {


  $scope.clazz = 'tabs-style-linebox';
  $scope.icon = 'fa-database';

  $scope.databases = null;
  $scope.$on('context:changed', function (evt, context) {
    $scope.$broadcast('db-chosen', {name: context, servers: $scope.databases[context]});
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

ee.controller('ClusterSingleDBController', function ($scope, Cluster) {

  $scope.$on('db-chosen', function (evt, db) {

    Cluster.database(db.name).then(function (data) {
      $scope.config = data;
      $scope.name = db.name;
    })


  })

  $scope.saveConfig = function () {

    Cluster.saveDBConfig({name: $scope.name, config: $scope.config}).then(function () {

    })

  }
})

ee.controller('EventsController', function ($scope, Plugins, $modal, Cluster, Profiler, Notification) {

  var PNAME = "ee-events";


  $scope.alertValues = [">=", "<="];

  $scope.parameters = ["value", "entries", "min", "max", "average", "total"];

  $scope.levels = ['OFFLINE', 'ONLINE'];

  $scope.eventWhen = ['MetricWhen', 'LogWhen'];

  $scope.eventWhat = ['MailWhat'];

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


  $scope.addEvent = function () {
    $scope.events.push({name: 'New Event', when: {name: $scope.eventWhen[0]}, what: {name: $scope.eventWhat[0]}});
  }
  $scope.dropEvent = function (e) {
    var idx = $scope.events.indexOf(e);
    $scope.events.splice(idx, 1);
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
          if (modalScope.metadata[data].type === 'CHRONO') {
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

ee.controller('MetricsController', function ($scope, Cluster) {

  $scope.clazz = 'tabs-style-linebox';


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
