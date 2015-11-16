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
