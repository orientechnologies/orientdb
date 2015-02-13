var API = '/';
var monitor = angular.module('monitor.services', ['ngResource']);
monitor.factory('Monitor', function ($http, $resource) {

  var db = "monitor"
  var resource = $resource(API + 'database/:database', {},
    {
      get: {
        method: 'GET',
        isArray: false,
        params: {
          database: db
        }
      }
    });

  resource.connect = function (username, password, callback, error) {
    $http.defaults.headers.common['Authorization'] = 'Basic ' + Base64.encode(username + ':' + password);
    $http.get(API + 'connect/' + db).success(callback).error(error);
  }

  resource.disconnect = function (callback) {
    $http.get(API + 'disconnect').success(function () {
      $http.defaults.headers.common['Authorization'] = null;
      callback();
    }).error(function () {
      $http.defaults.headers.common['Authorization'] = null;
      callback();
    });
  }
  resource.getServers = function (callback) {
    var query = 'select * from Server fetchPlan *:1'
    $http.post(API + 'command/monitor/sql/-/-1', query).success(function (data) {
      callback(data);
    });
  }
  resource.updateServer = function (server, callback) {

    if (server['@rid'].replace("#", '') == '-1:-1') {
      $http.post(API + 'monitoredServer/monitor/-1:-1', server).success(function (data) {
        callback(data);
      });
    } else {
      $http.put(API + 'monitoredServer/monitor/' + server['@rid'].replace("#", ''), server).success(function (data) {
        callback(data);
      });
    }

  }

  resource.getOnlineServers = function (callback) {
    var query = "select * from Server where status = 'ONLINE'";
    $http.post(API + 'command/monitor/sql/-/-1', query).success(function (data) {
      callback(data);
    });
  }
  resource.getServer = function (sId, callback) {

    $http.get(API + 'document/monitor/' + sId.replace('#', '')).success(function (data) {
      callback(data);
    });
  }
  return resource;
});
monitor.factory('Notification', function ($http, $resource) {
  var resource = $resource(API + 'database/:database');


  resource.latest = function (callback) {
    var query = 'select * from Log order by date desc fetchPlan *:1'
    $http.post(API + 'command/monitor/sql/-/5', query).success(function (data) {
      callback(data);
    });
  }
  return resource;
});
monitor.factory('Metric', function ($http, $resource) {

  var cachedMetrics = {};
  var resource = $resource(API + 'metrics/monitor/:server/:databases/:type/:kind/:names/:limit/:compress/:from/:to', {},
    {
      get: {
        method: 'GET',
        isArray: false,
        params: {
          databases: "all"
        }
      }
    }
  );


  resource.delete = function (params, callback) {
    var url = API + 'metrics/monitor/' + params.server + '/' + params.type + '/' + params.names;
    $http.delete(url).success(function (data) {
      callback(data);
    })
  }
  resource.getMetricTypes = function (params, callback) {

    var type = params.type;
    var enabled = params.enabled;
    var url = API + 'command/monitor/sql/-/-1';
    if (type) {
      var query = 'select * from Dictionary where enabled = ' + enabled + ' type = "' + type + '" order by name';
    } else {
      var query = 'select * from Dictionary where enabled = true  order by name ';
    }
    $http.post(url, query).success(function (data) {
      callback(data);
    })
  }
  resource.getMetricDictionary = function (callback) {

    var url = API + 'command/monitor/sql/-/-1';

    var query = 'select * from Dictionary order by name ';
    $http.post(url, query).success(function (data) {
      callback(data);
    })
  }
  resource.getMetricWithName = function (name, callback) {
    var url = API + 'command/monitor/sql/-/-1';
    if (name && !cachedMetrics[name]) {
      var query = 'select * from Dictionary where name = "' + name + '" ';
      $http.post(url, query).success(function (data) {
        cachedMetrics[data.result[0].name] = data.result[0];
        callback(data.result[0]);
      })
    } else {
      callback(cachedMetrics[name]);
    }

  }
  resource.getMetrics = function (params, callback) {
    var limit = params.limit || -1;
    var url = API + 'command/monitor/sql/-/' + limit;
    var query = "select @class, snapshot.dateTo as dateTo,snapshot.dateFrom as dateFrom, name, entries, last, min, max, average,value,total from Metric where ";
    if (params.names) {
      var like = "";
      params.name = "";
      params.names.forEach(function (elem, idx, array) {
        params.name += "'" + elem + "',";
        like += "name like '%" + elem + "%' ";
        if (idx < array.length - 1) {
          like += " OR ";
        }
      });
      var index = params.name.lastIndexOf(",");
      query += '( ' + like + ' ) ';
      //params.name = params.name.substring(0, index);

    } else {

    }


    if (params.dateFrom) {
      query += "and snapshot.dateFrom >= '{{dateFrom}}' ";
    }
    if (params.server) {
      query += "and snapshot.server = '{{server}}'";
    }
    if (params.dateTo) {
      query += "and snapshot.dateTo <= '{{dateTo}}' ";
    }
    query += " order by dateTo desc, name desc ";
    if (params.name && params.server) {
      query = S(query).template(params).s;
      $http.post(url, query).success(function (data) {
        callback(data);
      });
    } else {
      throw 'name and server params required';
    }
  }
  return resource;
});
monitor.factory('Server', function ($http, $resource, Metric, $q) {
  var resource = $resource(API + 'metrics/monitor/:server/:type/:kind/:names');

  resource.delete = function (name, callback) {
    var url = API + 'monitoredServer/monitor/' + name;
    $http.delete(url).success(function (data) {
      callback(data);
    })
  }
  resource.connect = function (server) {
    var deferred = $q.defer();
    var url = API + 'monitoredServer/monitor/' + server.name + '/connect';
    $http.post(url, {}).success(function () {
      deferred.resolve();
    });
    return deferred.promise;
  }
  resource.disconnect = function (server) {
    var deferred = $q.defer();
    var url = API + 'monitoredServer/monitor/' + server.name + '/disconnect';
    $http.post(url, {}).success(function () {
      deferred.resolve();
    });
    return deferred.promise;

  }
  resource.isAlive = function (server, callback, error) {
    var url = API + 'passThrough/monitor/' + server.name + '/alive';
    var name = server['@class']
    delete server['@class'];
    $http.post(url, server).success(function (data) {

      callback(data);
    }).error(function (data) {
      error(data);
    });
    server['@class'] = name;
  }
  resource.getStats = function (server) {
    var deferred = $q.defer();
    var url = API + 'serverStats/monitor' + (server != null ? "/" + server.name : "");
    $http.get(url).success(function (data) {
      deferred.resolve(data);
    });
    return deferred.promise;
  }
  resource.getConfiguration = function (server, callback, error) {
    var url = API + 'configuration/monitor/' + server.name + '';

    $http.get(url).success(function (data) {
      callback(data);
    }).error(function (data) {
//                error(data);
    });
  }
  resource.backUpDb = function (server, db, callback, error) {
    var url = API + 'passThrough/monitor/' + server.name + '/backup/' + db;
    window.open(url);
  }
  resource.saveConfiguration = function (server, config, callback, error) {
    var url = API + 'configuration/monitor/' + server.name + '';

    $http.put(url, config).success(function (data) {
      callback(data);
    }).error(function (data) {
//                error(data);
    });
  }
  resource.findDatabases = function (server, callback, error) {
    var params = {server: server, type: 'realtime', kind: 'information', names: 'system.databases'};
    Metric.get(params, function (data) {
      var databases = data.result[0]['system.databases'].split(",");
      callback(databases)
    }, error);
  }
  resource.findDatabasesOnSnapshot = function (server, callback) {
    var params = {server: server, type: 'snapshot', kind: 'information', names: 'system.databases'};
    Metric.get(params, function (data) {
      if (data.result[0] && data.result[0]['value']) {
        var databases = data.result[0]['value'].split(",");
      } else {
        var databases = [];
      }
      callback(databases)
    });
  }

  return resource;
});

monitor.factory('MetricConfig', function ($http, $resource) {
  var resource = $resource(API + 'database/:database');


  resource.getAll = function (callback, plan) {

    plan = plan || "";
    var query = 'select * from MetricConfig';
    if (plan != "") {
      query += " fetchPlan" + plan
    }
    $http.post(API + 'command/monitor/sql/-/-1', query).success(function (data) {
      callback(data);
    });
  }
  resource.create = function () {
    var obj = {};
    obj['@rid'] = '#-1:-1';
    obj['@class'] = 'MetricConfig';
    return obj;
  }
  resource.saveConfig = function (config, callback) {
    if (config['@rid'].replace("#", '') == '-1:-1') {
      $http.post(API + 'document/monitor/-1:-1', config).success(function (data) {
        callback(data);
      }).error(function (error) {
        callback(error);
      });

    } else {
      $http.put(API + 'document/monitor/' + config['@rid'].replace("#", ''), config).success(function (data) {
        callback(data);
      }).error(function (error) {
        callback(error);
      });
    }
  }

  resource.saveConfigs = function (configs, callback) {

    if (config['@rid'].replace("#", '') == '-1:-1') {
      $http.post(API + 'document/monitor/-1:-1', config).success(function (data) {
        callback(data);
      }).error(function (error) {
        callback(error);
      });

    } else {
      $http.put(API + 'document/monitor/' + config['@rid'].replace("#", ''), config).success(function (data) {
        callback(data);
      }).error(function (error) {
        callback(error);
      });
    }
  }

  resource.deleteConfig = function (config, callback) {

    $http.delete(API + 'document/monitor/' + config['@rid'].replace("#", '')).success(function (data) {
      callback(data);
    });

  }
  return resource;
});

monitor.factory('Settings', function ($http, $resource) {
  var resource = $resource(API + 'database/:database');

  resource.get = function (callback) {
    var url = API + 'loggedUserInfo/monitor/configuration';
    $http.get(url).success(function (data) {
      callback(data);
    });
  }
  resource.new = function () {
    var conf = {};
    conf['@class'] = 'UserConfiguration';
    conf['metrics'] = new Array;
    conf.grid = 1;
    return conf;
  }
  resource.put = function (config, callback) {
    var url = API + 'loggedUserInfo/monitor/configuration';
    $http.post(url, config).success(function (data) {
      callback(data);
    });
  }
  return resource;
});
monitor.factory('Users', function ($http, $resource) {
  var resource = $resource(API + 'database/:database');

  resource.savePasswd = function (ouser, callback, err) {
    $http.post(API + 'loggedUserInfo/monitor/changePassword', ouser).success(function (data) {
      callback(data);
    }).error(function (error) {
      err(error);
    });
  }
  resource.getWithUsername = function (username, callback) {
    var query = 'select * from OUser where name = "' + username + '"';
    $http.post(API + 'command/monitor/sql/-/-1', query).success(function (data) {
      if (data.result[0])
        callback(data.result[0]);
    });
  }
  return resource;
});

monitor.factory('Cluster', function ($http, $resource, $q) {
  var resource = $resource(API + 'database/:database');

  resource.delete = function (name, callback) {
    var url = API + 'distributed/monitor/configuration/' + name;
    $http.delete(url).success(function (data) {
      callback(data);
    })
  }

  resource.connect = function (name) {
    var deferred = $q.defer();
    var url = API + 'distributed/monitor/connect/' + name;
    $http.get(url).success(function (data) {
      deferred.resolve(data);
    });
    return deferred.promise;
  }
  resource.disconnect = function (name) {
    var deferred = $q.defer();
    var url = API + 'distributed/monitor/disconnect/' + name;
    $http.get(url).success(function (data) {
      deferred.resolve(data);
    });
    return deferred.promise;
  }
  resource.saveCluster = function (cluster) {
    var deferred = $q.defer();
    var url = API + 'distributed/monitor/configuration';
    $http.post(url, cluster).success(function (data) {
      deferred.resolve(data);
    });
    return deferred.promise;
  }

  resource.getServers = function (cluster) {
    var deferred = $q.defer();
    var cId = cluster['@rid'];
    var query = 'select * from Server where cluster = {{cluster}}';
    query = S(query).template({cluster: cId}).s;
    $http.post(API + 'command/monitor/sql/-/-1', query).success(function (data) {
      deferred.resolve(data.result);
    });
    return deferred.promise;
  }
  resource.getAll = function () {
    var deferred = $q.defer();

    var query = 'select * from Cluster';
    $http.post(API + 'command/monitor/sql/-/-1', query).success(function (data) {
      deferred.resolve(data.result);
    });
    return deferred.promise;
  }
  resource.getDistributedInfo = function (type) {
    var deferred = $q.defer();
    var url = API + 'distributed/monitor/' + type;
    $http.get(url).success(function (data) {
      deferred.resolve(data);
    });
    return deferred.promise;
  }
  resource.getClusterDbInfo = function (cluster, db) {
    var deferred = $q.defer();
    var url = API + 'distributed/monitor/dbconfig/{{cluster}}/{{db}}';
    url = S(url).template({cluster: cluster, db: db}).s;
    $http.get(url).success(function (data) {
      deferred.resolve(data);
    }).error(function (data) {
      deferred.reject(data);
    });
    return deferred.promise;
  }
  resource.saveClusterDbInfo = function (cluster, db, config) {
    var deferred = $q.defer();
    var url = API + 'distributed/monitor/dbconfig/{{cluster}}/{{db}}';
    url = S(url).template({cluster: cluster, db: db}).s;
    $http.post(url, config).success(function (data) {
      deferred.resolve(data);
    });
    return deferred.promise;
  }
  resource.deployDb = function (cluster, server, db) {
    var deferred = $q.defer();
    var url = API + 'distributed/monitor/deploy/{{cluster}}/{{server}}/{{db}}';
    url = S(url).template({cluster: cluster, server: server, db: db}).s;
    $http.get(url).success(function (data) {
      deferred.resolve(data);
    }).error(function (data) {
      deferred.reject(data);
    });
    return deferred.promise;
  }
  return resource;
});

monitor.factory('ContextNotification', function ($timeout) {

  return {
    notifications: new Array,
    errors: new Array,

    push: function (notification) {
      this.notifications.splice(0, this.notifications.length);
      this.errors.splice(0, this.errors.length);

      if (notification.error) {
        this.errors.push(notification);
      } else {
        this.notifications.push(notification);
      }
      var that = this;
      $timeout(function () {
        that.clear();
      }, 5000);
    },
    clear: function () {
      this.notifications.splice(0, this.notifications.length);
      this.errors.splice(0, this.errors.length);
    }

  }
});
monitor.factory('StickyNotification', function ($timeout) {

  return {
    notifications: new Array,
    errors: new Array,

    push: function (notification) {
      this.notifications.splice(0, this.notifications.length);
      this.errors.splice(0, this.errors.length);

      if (notification.error) {
        this.errors.push(notification);
      } else {
        this.notifications.push(notification);
      }
    },
    clear: function () {
      this.notifications.splice(0, this.notifications.length);
      this.errors.splice(0, this.errors.length);
    }

  }
});
