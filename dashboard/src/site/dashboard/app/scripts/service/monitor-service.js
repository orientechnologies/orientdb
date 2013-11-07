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
        $http.post(API + 'command/monitor/sql/-/5', query).success(function (data) {
            callback(data);
        });
    }
    return resource;
});
monitor.factory('Metric', function ($http, $resource) {

    var cachedMetrics = {};
    var resource = $resource(API + 'metrics/monitor/:server/:databases/:type/:kind/:names/:limit/:compress/:from/:to', { },
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
    resource.getMetricTypes = function (type, callback) {
        var url = API + 'command/monitor/sql/-/-1';
        if (type) {
            var query = 'select * from Dictionary where type = "' + type + '" order by name';
        } else {
            var query = 'select * from Dictionary order by name';
        }
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
monitor.factory('Server', function ($http, $resource, Metric) {
    var resource = $resource(API + 'metrics/monitor/:server/:type/:kind/:names');

    resource.delete = function (name, callback) {
        var url = API + 'monitoredServer/monitor/' + name;
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
    resource.getConfiguration = function (server, callback, error) {
        var url = API + 'configuration/monitor/' + server.name + '';

        $http.get(url).success(function (data) {
            callback(data);
        }).error(function (data) {
//                error(data);
            });
    }
    resource.saveConfiguration = function (server, config, callback, error) {
        var url = API + 'configuration/monitor/' + server.name + '';

        $http.put(url, config).success(function (data) {
            callback(data);
        }).error(function (data) {
//                error(data);
            });
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

monitor.factory('MetricConfig', function ($http, $resource) {
    var resource = $resource(API + 'database/:database');


    resource.getAll = function (callback, plan) {

        plan = plan || "";
        var query = 'select * from MetricConfig fetchPlan' + plan
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