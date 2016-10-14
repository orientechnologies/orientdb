var ee = angular.module('ee.services', []);

ee.factory('CommandLogApi', function ($http, $resource, DatabaseApi) {

  var current = {
    name: null,
    username: null,
    metadata: null
  }

  var header = ["@rid", "@version", "@class"];
  var exclude = ["@rid", "@version", "@class", "@type", "@fieldTypes", "$then", "$resolved"];

  var resource = $resource($http);
  resource.getLogs = function (params, callback) {

    var datefrom = params.dateFrom;

    $http.get(API + '/log/tail/' + datefrom).success(function (data) {
    }).error(function (data) {
    })
  }
  resource.getLastLogs = function (params, callback) {
    var searchValue = '';
    var logtype = '';
    var dateFrom = '';
    var hourFrom = '';
    var dateTo = '';
    var hourTo = '';
    var file = '';
    var server = '&node=' + encodeURIComponent(params.server);

    if (params.searchvalue) {
      searchValue = '&searchvalue=' + params.searchvalue
    }
    if (params.logtype) {
      logtype = '&logtype=' + params.logtype;
    }
    if (params.dateFrom != undefined) {
      dateFrom = new Date(params.dateFrom);
      dateFrom = '&dateFrom=' + dateFrom.getTime();
    }
    if (params.hourFrom != undefined) {

      hourFrom = '&hourFrom=' + params.hourFrom;
    }
    if (params.dateTo != undefined) {
      dateTo = new Date(params.dateTo);
      dateTo = '&dateTo=' + dateTo.getTime();
    }
    if (params.hourTo != undefined) {

      hourTo = '&hourTo=' + params.hourTo;
    }
    if (params.file != undefined) {

      file = '&file=' + params.file;

    }
    $http.get(API + 'log/' + params.typeofSearch + '?' + 'tail=100000' + server + searchValue + logtype + dateFrom + hourFrom + dateTo + hourTo + file).success(function (data) {
      callback(data);
    }).error(function (data) {
      callback(data);

    })
  }

  resource.getListFiles = function (params, callback) {
    var server = params.server;
    var url = API + 'log/files?node=' + encodeURIComponent(server);
    $http.get(url).success(function (data) {
      callback(data);
    }).error(function (data) {
    })
  }

  resource.purge = function (params, callback) {

    var type = params.type;
    $http.get('/purge/monitor/' + type).success(function (data) {

    }).error(function (data) {

    })
  }
  return resource;
});


ee.factory('Cluster', function ($http, $resource, $q) {


  var resource = $resource('');


  resource.node = function () {

    var deferred = $q.defer();
    var text = API + 'distributed/node';
    $http.get(text).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });
    return deferred.promise;
  }


  resource.backUp = function (server, db) {

    var url = API + 'backup/' + db;

    if (server) {
      url += "?node=" + server.name;
    }
    window.open(url);
  }


  /*
   Returns the config file for Server
   */
  resource.configFile = function (server) {
    var deferred = $q.defer();

    var url = API + 'configuration';
    if (server && server.name) {
      url += '?node=' + server.name;
    }
    $http.get(url).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });

    return deferred.promise;
  }
  /*
   Returns Info for Server
   */
  resource.infoServer = function (server) {

    var deferred = $q.defer();
    var url = API + 'node/info';
    if (server && server.name) {
      url += '?node=' + server.name;
    }
    $http.get(url).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });
    return deferred.promise;
  }
  /*
   Returns Database Distributed Config
   */
  resource.database = function (db) {

    var deferred = $q.defer();
    var text = API + 'distributed/database/' + db;
    $http.get(text).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });
    return deferred.promise;
  }


  /*
   Kill Connection n for server
   */

  resource.killConnection = function (server, n, callback) {
    var deferred = $q.defer();
    var url = API + 'node/connection/kill/' + n;
    if (server && server.name) {
      url += '?node=' + server.name;
    }
    $http.post(url).success(function () {
      deferred.resolve()
    });
    return deferred.promise;
  }
  /*
   Interrupt Connection n for server
   */
  resource.interruptConnection = function (server, n, callback) {
    var deferred = $q.defer();
    var url = API + 'node/connection/interrupt' + n;
    if (server && server.name) {
      url += '?node=' + server.name;
    }
    $http.post(url).success(function () {
      deferred.resolve()
    });
    return deferred.promise;
  }
  resource.saveDBConfig = function (params) {

    var deferred = $q.defer();
    var url = API + 'distributed/database/' + params.name;
    $http.put(url, params.config).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });
    return deferred.promise;
  }
  /*
   Returns Distributed Stats
   */
  resource.stats = function (name) {

    var deferred = $q.defer();
    var text = API + 'distributed/stats' + (name ? ("/" + name) : "");
    $http.get(text).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });
    return deferred.promise;
  }


  return resource
});


ee.factory('Profiler', function ($http, $resource, $q) {


  var resource = $resource('');


  resource.profilerData = function (params) {

    var deferred = $q.defer();
    var text = API + 'sqlProfiler/' + params.db;

    if (params.server) {
      text += "?node=" + params.server;
    }
    $http.get(text).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });
    return deferred.promise;
  }
  resource.reset = function (params) {

    var deferred = $q.defer();
    var text = API + 'sqlProfiler/' + params.db + '/reset';

    if (params.server) {
      text += "?node=" + params.server;
    }
    $http.post(text).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });
    return deferred.promise;
  }

  resource.metadata = function () {
    var deferred = $q.defer();
    var text = API + 'profiler/metadata';
    $http.get(text).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });
    return deferred.promise;
  }
  resource.realtime = function () {
    var deferred = $q.defer();
    var text = API + 'profiler/realtime';
    $http.get(text).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });
    return deferred.promise;
  }
  return resource
});


ee.factory('CommandCache', function ($http, $q) {


  var resource = {}


  /*
   Get Cache configuration
   */
  resource.config = function (params) {

    var deferred = $q.defer();
    var text = API + 'commandCache/' + params.db;
    if (params.server) {
      text += "?node=" + params.server;
    }
    $http.get(text).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });
    return deferred.promise;
  }

  /*
   Get Cache configuration
   */
  resource.purge = function (params) {

    var deferred = $q.defer();
    var text = API + 'commandCache/' + params.db + '/purge';
    if (params.server) {
      text += "?node=" + params.server;
    }
    $http.post(text).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });
    return deferred.promise;
  }

  /*
   Get Cache results
   */
  resource.results = function (params) {

    var deferred = $q.defer();
    var text = API + 'commandCache/' + params.db + '/results';
    if (params.server) {
      text += "?node=" + params.server;
    }
    $http.get(text).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });
    return deferred.promise;
  }

  /*
   Get Cache results set
   */
  resource.queryResults = function (params) {
    var deferred = $q.defer();
    var text = API + 'commandCache/' + params.db + '/results';
    if (params.server) {
      text += "?node=" + params.server;
    }
    $http.post(text, {query: params.query}).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });
    return deferred.promise;
  }
  /*
   Save Cache configuration
   */
  resource.saveConfig = function (params) {

    var deferred = $q.defer();
    var text = API + 'commandCache/' + params.db;
    if (params.server) {
      text += "?node=" + params.server;
    }
    $http.put(text, params.config).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });
    return deferred.promise;
  }

  /*
   Enable cache
   */
  resource.enable = function (params) {
    var deferred = $q.defer();
    var url = API + 'commandCache/' + params.db + "/enable";
    if (params.server) {
      url += "?node=" + params.server;
    }
    $http.put(url).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });
    return deferred.promise;
  }

  /*
   Disable cache
   */
  resource.disable = function (params) {
    var deferred = $q.defer();
    var url = API + 'commandCache/' + params.db + "/disable";
    if (params.server) {
      url += "?node=" + params.server;
    }
    $http.put(url).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });
    return deferred.promise;
  }
  return resource;

});

ee.factory('Auditing', function ($http, $resource, $q, CommandApi) {

  var resource = $resource('');


  resource.getConfig = function (params) {

    var deferred = $q.defer();
    var text = API + 'auditing/' + params.db + "/config";
    $http.get(text).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });
    return deferred.promise;
  }
  resource.saveConfig = function (params, cfg) {
    var deferred = $q.defer();
    var text = API + 'auditing/' + params.db + "/config";
    $http.post(text, cfg).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });
    return deferred.promise;
  }
  resource.query = function (params) {

    var deferred = $q.defer();


    var text = API + 'auditing/logs/query';
    $http.post(text, params.query).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });


    return deferred.promise;
  }
  return resource
});


ee.factory('Plugins', function ($http, $q) {


  var plugins = {}

  plugins.all = function (server) {

    var deferred = $q.defer();
    var url = API + 'plugins';
    if (server) {
      url += '?node=' + server;
    }
    $http.get(url).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });
    return deferred.promise;

  }

  plugins.one = function (params) {
    var deferred = $q.defer();
    var url = API + 'plugins/' + params.plugin;
    if (params.server) {
      url += '?node=' + params.server;
    }
    $http.get(url).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });
    return deferred.promise;

  }
  plugins.saveConfig = function (server, name, config) {
    var deferred = $q.defer();
    var url = API + 'plugins/' + name;
    if (server) {
      url += '?node=' + server;
    }
    $http.put(url, config).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });
    return deferred.promise;
  }
  return plugins;
})

ee.factory('Teleporter', function ($http, $q) {


  var teleporter = {}


  teleporter.launch = function (params) {

    var deferred = $q.defer();
    var url = API + 'teleporter/job';
    if (params.server) {
      url += '?node=' + params.server;
    }
    $http.post(url, params.config).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });
    return deferred.promise;
  };

  teleporter.test = function (params) {

    var deferred = $q.defer();
    var url = API + 'teleporter/test';
    if (params.server) {
      url += '?node=' + params.server;
    }
    $http.post(url, params.config).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });
    return deferred.promise;
  };

  teleporter.drivers = function (params) {
    var deferred = $q.defer();
    var url = API + 'teleporter/drivers';
    if (params.server) {
      url += '?node=' + params.server;
    }
    $http.get(url).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });
    return deferred.promise;
  }
  teleporter.status = function (params) {

    var deferred = $q.defer();
    var url = API + 'teleporter/status';
    if (params.server) {
      url += '?node=' + params.server;
    }
    $http.get(url).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });
    return deferred.promise;
  }
  return teleporter;
})

ee.factory("AgentService", function (Profiler, $q) {
  var agent = {
    active: null
  }
  agent.isActive = function () {

    var deferred = $q.defer();
    if (agent.active == null) {
      Profiler.metadata().then(function (data) {
        agent.active = true;
        deferred.resolve();
      }).catch(function (err) {
        agent.active = false;
        deferred.resolve();
      })
    } else {
      deferred.resolve();
    }
    return deferred.promise;
  }
  return agent;
})

ee.factory("BackupService", function (Profiler, $q, $http) {
  var backups = {}


  backups.restore = function (uuid, restored) {

    var deferred = $q.defer();
    var url = API + 'backupManager/' + uuid + "/restore";
    $http.post(url, restored).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });
    return deferred.promise;
  }

  backups.remove = function (uuid, restored) {
    var deferred = $q.defer();
    var url = API + 'backupManager/' + uuid + "/remove?unitId=" + restored.unitId + '&txId=' + restored.log.txId;
    $http.delete(url).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });
    return deferred.promise;
  }
  backups.get = function () {
    var deferred = $q.defer();
    var url = API + 'backupManager';
    $http.get(url).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });
    return deferred.promise;
  }


  backups.logs = function (uuid, params) {
    var deferred = $q.defer();
    var url = API + 'backupManager/' + uuid + "/log";

    if (params) {
      url += serialize(params);
    }
    $http.get(url).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });
    return deferred.promise;
  }
  backups.unitLogs = function (uuid, unitId, params) {
    var deferred = $q.defer();
    var url = API + 'backupManager/' + uuid + "/log/" + unitId;

    if (params) {
      url += serialize(params);
    }
    $http.get(url).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });

    return deferred.promise;
  }
  function serialize(obj) {
    return '?' + Object.keys(obj).reduce(function (a, k) {
        a.push(k + '=' + encodeURIComponent(obj[k]));
        return a
      }, []).join('&')
  }

  backups.save = function (backup) {
    var deferred = $q.defer();
    var url = API + 'backupManager';

    if (backup.uuid) {
      $http.put(url + "/" + backup.uuid, backup).success(function (data) {
        deferred.resolve(data)
      }).error(function (data, status, headers, config) {
        deferred.reject({data: data, status: status});
      });
    } else {
      $http.post(url, backup).success(function (data) {
        deferred.resolve(data)
      }).error(function (data, status, headers, config) {
        deferred.reject({data: data, status: status});
      });
    }

    return deferred.promise;
  }
  return backups;
})


ee.factory("ThreadService", function ($q, $http) {
  var threads = {};

  threads.dump = function (server) {
    var deferred = $q.defer();
    var url = API + 'node/threadDump';
    if (server && server.name) {
      url += '?node=' + server.name;
    }
    $http.get(url).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });
    return deferred.promise;
  }
  return threads;
})
ee.factory("BackupCalendar", function () {

  var calendar = {};
  calendar.init = function () {

  }
  return calendar;

})


ee.factory("ChartHelper", function () {

  var helper = {};
  helper.countOps = function (array, regexp) {

    var keys = Object.keys(array).filter(function (k) {
      return k.match(regexp) != null;
    })

    var ops = 0;
    keys.forEach(function (k) {
      ops += array[k];
    });
    return ops;
  }
  helper.countChronos = function (array, regexp) {

    var keys = Object.keys(array).filter(function (k) {
      return k.match(regexp) != null;
    })

    var ops = 0;
    keys.forEach(function (k) {
      ops += array[k].entries;
    });
    return ops;
  }

  helper.serverStatsHeader = [{
    name: 'Create',
    description: '',
    transform: function (stats) {
      try {
        var val = helper.countOps(stats['realtime']['counters'], /db.*createRecord/g);
        return val;
      } catch (e) {
        return 0;
      }
    }
  }, {
    name: 'Read',
    description: '',
    transform: function (stats) {
      try {
        var val = helper.countOps(stats['realtime']['counters'], /db.*readRecord/g);
        return val;
      } catch (e) {
        return 0;
      }
    }
  }, {
    name: 'Scan',
    description: 'Scan records',
    transform: function (stats) {
      try {
        var val = helper.countOps(stats['realtime']['counters'], /db.*scanRecord/g);
        return val;
      } catch (e) {
        return 0;
      }
    }
  }, {
    name: 'Update',
    description: '',
    transform: function (stats) {
      try {
        var val = helper.countOps(stats['realtime']['counters'], /db.*updateRecord/g);
        return val;
      } catch (e) {
        return 0;
      }
    }
  }, {
    name: 'Delete',
    description: '',
    transform: function (stats) {
      try {
        var val = helper.countOps(stats['realtime']['counters'], /db.*deleteRecord/g);
        return val;
      } catch (e) {
        return 0;
      }
    }
  }, {
    name: 'Conflict',
    description: '',
    transform: function (stats) {
      try {
        var val = helper.countOps(stats['realtime']['counters'], /db.*conflictRecord/g);
        return val;
      } catch (e) {
        return 0;
      }
    }
  }, {
    name: 'Tx Commit',
    description: '',
    transform: function (stats) {
      try {
        var val = helper.countOps(stats['realtime']['counters'], /db.*txCommit/g);
        return val;
      } catch (e) {
        return 0;
      }
    }
  }, {
    name: 'Tx Rollback',
    description: '',
    transform: function (stats) {
      try {
        var val = helper.countOps(stats['realtime']['counters'], /db.*txRollback/g);
        return val;
      } catch (e) {
        return 0;
      }
    }
  }, {
    name: 'Distributed Tx Retries',
    description: '',
    transform: function (stats) {
      try {
        var val = helper.countOps(stats['realtime']['counters'], /db.*distributedTxRetries/g);
        return val;
      } catch (e) {
        return 0;
      }
    }
  }]
  return helper;

})

database.factory('HaCommand', function ($http, $resource, $q) {


  var resource = $resource('');


  resource.removeNode = function (database, server) {

    var deferred = $q.defer();
    var text = API + 'command/' + database + '/sql/-/-1?format=rid,type,version,class,graph';
    var query = "Ha remove server {{name}}"
    var queryText = S(query).template({name: server}).s;
    $http.post(text, queryText).success(function (data) {
      deferred.resolve(data)
    }).error(function (data) {
      deferred.reject(data);
    });
    return deferred.promise;
  }
  resource.stopNode = function (server) {

    var deferred = $q.defer();
    var url = API + 'distributed/stop/' + server;
    if (server) {
      url += '?node=' + server;
    }
    $http.post(url).success(function (data) {
      deferred.resolve(data)
    }).error(function (data) {
      deferred.reject(data);
    });
    return deferred.promise;
  }
  resource.restartNode = function (server) {

    var deferred = $q.defer();
    var url = API + 'distributed/restart/' + server;
    if (server) {
      url += '?node=' + server;
    }
    $http.post(url).success(function (data) {
      deferred.resolve(data)
    }).error(function (data) {
      deferred.reject(data);
    });
    return deferred.promise;
  }

  resource.syncCluster = function (server, database, cluster) {

    var deferred = $q.defer();
    var url = API + 'distributed/syncCluster/' + database + '/' + cluster;
    if (server) {
      url += '?node=' + server;
    }
    $http.post(encodeURI(url), null).success(function (data) {
      deferred.resolve(data)
    }).error(function (data) {
      deferred.reject(data);
    });
    return deferred.promise;
  }
  resource.syncDatabase = function (server, database) {

    var deferred = $q.defer();
    var url = API + 'distributed/syncDatabase/' + database;
    if (server) {
      url += '?node=' + server;
    }
    $http.post(encodeURI(url), null).success(function (data) {
      deferred.resolve(data)
    }).error(function (data) {
      deferred.reject(data);
    });
    return deferred.promise;
  }
  resource.stopReplication = function (server, database) {

    var deferred = $q.defer();
    var url = API + 'distributed/stopReplication/' + database;
    if (server) {
      url += '?node=' + server;
    }
    $http.post(encodeURI(url), null).success(function (data) {
      deferred.resolve(data)
    }).error(function (data) {
      deferred.reject(data);
    });
    return deferred.promise;
  }
  resource.startReplication = function (server, database) {

    var deferred = $q.defer();
    var url = API + 'distributed/startReplication/' + database;
    if (server) {
      url += '?node=' + server;
    }
    $http.post(encodeURI(url), null).success(function (data) {
      deferred.resolve(data)
    }).error(function (data) {
      deferred.reject(data);
    });
    return deferred.promise;
  }
  return resource
});

ee.factory("SecurityService", function (Profiler, $q, $http) {
  var config = {}


  config.get = function () {
    var deferred = $q.defer();
    var url = API + 'security/config';
    $http.get(url).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });
    return deferred.promise;
  }

  config.reload = function (config) {
    var deferred = $q.defer();
    var url = API + 'security/reload';
    $http.post(url, config).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });
    return deferred.promise;
  }
  return config;
})
var AgentResolve = {
  current: function (AgentService, $q) {
    var deferred = $q.defer();


    AgentService.isActive().then(function () {
      deferred.resolve();
    })
    return deferred.promise;
  },
  delay: function ($q, $timeout) {
    var delay = $q.defer();
    $timeout(delay.resolve, 0);
    return delay.promise;
  }
}
