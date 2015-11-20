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
      console.log(params.file);
    }
    $http.get(API + '/log/' + params.typeofSearch + '?' + 'tail=100000' + server + searchValue + logtype + dateFrom + hourFrom + dateTo + hourTo + file).success(function (data) {
      callback(data);
    }).error(function (data) {
      callback(data);

    })
  }

  resource.getListFiles = function (params, callback) {
    var server = params.server;

    $http.get(API + '/log/files?node=' + encodeURIComponent(server)).success(function (data) {
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
   Returns Database Diistributed Config
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
  resource.query = function (params, args) {

    var deferred = $q.defer();


    var text = API + 'auditing/' + params.db + "/query";
    $http.post(text, args).success(function (data) {
      deferred.resolve(data)
    }).error(function (data, status, headers, config) {
      deferred.reject({data: data, status: status});
    });

    //var query = "select from {{clazz}} {{where}} order by date desc limit {{limit}} fetchPlan user:1";
    //args.where = "";
    //
    //if (args.user) {
    //  args.where += " user.name = '{{user}}' and"
    //}
    //if (args.record) {
    //  args.where += " record  = '{{record}}' and"
    //}
    //if (args.operation) {
    //  args.where += " operation  = '{{operation}}' and"
    //}
    //if (args.from) {
    //  args.where += " date  > '{{from}}' and"
    //}
    //if (args.to) {
    //  args.where += " date  < '{{to}}' and"
    //}
    //if (args.note) {
    //  args.where += " note like '%{{note}}%' and"
    //}
    //if (args.where != "") {
    //  var n = args.where.lastIndexOf("and");
    //  args.where = " where " + args.where.substring(0, n);
    //}
    //var queryText = S(query).template(args).s;
    //if (args.where != "") {
    //  queryText = S(queryText).template(args).s
    //}
    //var params = {
    //  database: database,
    //  text: queryText,
    //  language: 'sql',
    //  verbose: false
    //}
    //CommandApi.queryText(params, function (data) {
    //  deferred.resolve(data)
    //}, function (data) {
    //  deferred.reject(data);
    //})


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
