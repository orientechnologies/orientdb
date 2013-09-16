var API = '/';
var monitor = angular.module('monitor.services', ['ngResource']);
monitor.factory('Monitor', function ($http, $resource) {

    var db = "monitor"
    var resource = $resource(API + 'database/:database');

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
        var query = 'select * from Server'
        $http.post(API + 'command/monitor/sql/-/-1', query).success(function (data) {
            callback(data);
        });
    }
    resource.updateServer = function (server, callback) {

        if (server['@rid'].replace("#", '') == '-1:-1') {
            $http.post(API + 'document/monitor/-1:-1', server).success(function (data) {
                callback(data);
            });
        } else {
            $http.put(API + 'document/monitor/' + server['@rid'].replace("#", ''), server).success(function (data) {
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
        $http.post(API + 'command/monitor/sql/-/10', query).success(function (data) {
            callback(data);
        });
    }
    return resource;
});
monitor.factory('Metric', function ($http, $resource) {
    var resource = $resource(API + 'metrics/monitor/:server/:type/:kind/:names');

    resource.delete = function (params, callback) {
        var url = API + 'metrics/monitor/' + params.server + '/' + params.type + '/' + params.names;
        $http.delete(url).success(function (data) {
            callback(data);
        })
    }
    resource.getMetricTypes = function (type, callback) {
        var url = API + 'command/monitor/sql/-/-1';
        var query = 'select * from Dictionary where type = "' + type + '" order by name';
        $http.post(url, query).success(function (data) {
            callback(data);
        })
    }
    resource.getMetrics = function (params, callback) {
        var url = API + 'command/monitor/sql/-/-1';
        var query = "select snapshot.dateTo as dateTo, name, entries, last, min, max, average, total from Metric where  name = '{{name}}' and  snapshot.server = '{{server}}'";
        if(params.dateFrom){
           query += "and snapshot.dateFrom >= {{dateFrom}} ";
        }
        if(params.dateTo){
            query += "and snapshot.dateTo <= {{dateTo}} ";
        }
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
monitor.factory('Server', function ($http, $resource, Metric) {
    var resource = $resource(API + 'metrics/monitor/:server/:type/:kind/:names');

    resource.delete = function (rid, callback) {
        var url = API + 'document/monitor/' + rid.replace('#', '');
        $http.delete(url).success(function (data) {
            callback(data);
        })
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
    resource.findDatabases = function (server, callback) {
        var params = {  server: server, type: 'realtime', kind: 'information', names: 'system.databases' };
        Metric.get(params, function (data) {
            var databases = data.result[0]['system.databases'].split(",");
            callback(databases)
        });
    }
    return resource;
});