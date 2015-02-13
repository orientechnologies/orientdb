/**
 * Created with JetBrains WebStorm.
 * User: marco
 * Date: 24/09/13
 * Time: 12.07
 * To change this template use File | Settings | File Templates.
 */
var biconsole = angular.module('workbench-logs.services', ['ngResource']);

biconsole.factory('DatabaseApi', function ($http, $resource) {

    var resource = $resource(API + 'database/:database');
    resource.listDatabases = function (callback) {
        $http.get(API + 'listDatabases').success(callback);
    }
    resource.connect = function (database, username, password, callback, error) {
        $http.defaults.headers.common['Authorization'] = 'Basic ' + Base64.encode(username + ':' + password);
        $http.get(API + 'connect/' + database).success(callback).error(error);
    }
    resource.createDatabase = function (name, type, stype, username, password, callback) {
        $http.defaults.headers.common['Authorization'] = 'Basic ' + Base64.encode(username + ':' + password);
        $http.post(API + 'database/' + name + "/" + stype + "/" + type).success(function (data) {
            $http.defaults.headers.common['Authorization'] = null;
            callback(data);
        });
    }

    resource.exportDatabase = function (database) {
        window.open(API + 'export/' + database);
    }
    resource.importDatabase = function (database, blob, file) {
        var fd = new FormData();
        fd.append("databaseFile", blob, file.name);
        $http.post(API + 'import/' + database, fd, { headers: { 'Content-Type': undefined }, transformRequest: angular.identity });
    }
    resource.getAllocation = function (database, callback) {
        $http.get(API + 'allocation/' + database).success(callback);
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
    return resource;
});


biconsole.factory('CommandLogApi', function ($http, $resource, DatabaseApi, $httpBackend) {

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

        $http.get('/log/tail/' + datefrom).success(function (data) {
        }).error(function (data) {
            })
    }

    resource.getMetadata = function () {
        return current.metadata;
    }
    resource.setMetadata = function (metadata) {
        current.metadata = metadata;
    }
    resource.getLastLogs = function (params, callback) {
        var searchValue = '';
        var logtype = '';
        var dateFrom = '';
        var hourFrom = '';
        var dateTo = '';
        var hourTo = '';
        var file = '';
        var server = '&name=' + encodeURIComponent(params.server);

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
        $http.get('/log/monitor/' + params.typeofSearch + '?' + 'tail=100000' + server + searchValue + logtype + dateFrom + hourFrom + dateTo + hourTo + file).success(function (data) {
            callback(data);
        }).error(function (data) {
                callback(data);

            })
    }

    resource.getListFiles = function (params, callback) {
        var server = params.server;

        $http.get('/log/monitor/files?name=' + encodeURIComponent(server)).success(function (data) {
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
    resource.notifyModifiedMetrics = function (params, callback) {

        var metrics = params.metrics;
        var text = '/notifymetrics/monitor/';
        $http.post(text, JSON.stringify(metrics)).success(function (data) {

        });

    }

    resource.queryText = function (params, callback, error) {
        var startTime = new Date().getTime();
        var limit = params.limit || 20;
        var verbose = params.verbose != undefined ? params.verbose : true;
        var shallow = params.shallow != undefined ? '' : ',shallow';
        var text = '/command/' + 'monitor' + "/" + params.language + "/-/" + limit + '?format=rid,type,version' + shallow + ',class,graph';
        if (params.text) {
            var query = params.text.trim();
            $http.post(text, query).success(function (data) {
                var time = ((new Date().getTime() - startTime) / 1000);
                var records = data.result ? data.result.length : "";
                if (verbose) {
                    var noti = "Query executed in " + time + " sec. Returned " + records + " record(s)";
                }
                if (data != undefined)
                    callback(data);
                else
                    callback('ok');
            }).error(function (data) {
                    if (error) {
                        error(data);
                    }
                });
        }
    }
    resource.listPropertiesForClass = function (clazz) {
        var metadata = this.getMetadata();
        var classes = metadata['classes'];
        var fields = new Array
        for (var entry in classes) {
            var defaultCluster = classes[entry]['properties'];
            if (clazz.toUpperCase() == classes[entry].name.toUpperCase()) {
                var props = classes[entry]['properties'];
                for (var f in props) {
                    console.log(props[f]['name']);
//                    var ff = { props[f]['name'] : props[f] }
                    fields.push(props[f]);
                }
                ;
                break;
            }
        }
        return fields;
    }
    resource.getPropertyTableFromResults = function (results) {
        var self = this;
        var headers = new Array;
        results.forEach(function (element, index, array) {
            var tmp = Object.keys(element);
            if (headers.length == 0) {
                headers = headers.concat(tmp);
            } else {
                var tmp2 = tmp.filter(function (element, index, array) {
                    return headers.indexOf(element) == -1;
                });
                headers = headers.concat(tmp2);
            }
        });
        var all = headers.filter(function (element, index, array) {
            return exclude.indexOf(element) == -1;
        });
        return all;
    }
    resource.listClasses = function () {
        var metadata = this.getMetadata();
        var classes = metadata['classes'];
        var fields = new Array
        for (var entry in classes) {
            var claq = classes[entry].name
            fields.push(classes[entry])
        }
        return fields;
    }
    resource.listClassesForSuperclass = function (superClazz) {
        var metadata = this.getMetadata();
        var classes = metadata['classes'];
        var fields = new Array
        for (var entry in classes) {
            var claq = classes[entry].name
            if (classes[entry].superClass == superClazz)
                fields.push(classes[entry])
        }
        return fields;
    }
    resource.refreshMetadata = function (database, callback) {
        var currentDb = DatabaseApi.get({database: 'monitor'}, function () {
            current.name = 'monitor';
            current.username = currentDb.currentUser;
            current.metadata = currentDb;
            if (currentDb != null)
                callback();
        });
    }
    return resource;
})
;




