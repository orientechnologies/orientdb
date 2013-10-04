/**
 * Created with JetBrains WebStorm.
 * User: marco
 * Date: 24/09/13
 * Time: 12.07
 * To change this template use File | Settings | File Templates.
 */
var biconsole = angular.module('workbench-logs.services', ['ngResource']);
biconsole.factory('CommandLogApi', function ($http, $resource) {


    var exclude =  ["@type", "@fieldTypes", "$then", "$resolved"];

    var resource = $resource($http);
    console.log(resource);
    resource.getLogs = function (params, callback) {

        var datefrom = params.dateFrom

        $http.get('/log/tail/' + datefrom).success(function (data) {
        }).error(function (data) {
//            console.log(data)

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
        var server = '&name=' + params.server;

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

        $http.get('/log/' + params.typeofSearch + '?' + 'tail=100000' + server + searchValue + logtype + dateFrom + hourFrom + dateTo + hourTo + file).success(function (data) {
            callback(data);
        }).error(function (data) {
            })
    }

    resource.getListFiles = function (params, callback) {
        var server = '?name=' + params.server;
        $http.get('/log/files' + server).success(function (data) {
            callback(data);
        }).error(function (data) {
            })
    }

    resource.queryText = function (params, callback, error) {
        var startTime = new Date().getTime();
        var limit = params.limit || 20;
        var verbose = params.verbose != undefined ? params.verbose : true;
        var shallow = params.shallow != undefined ? '' : ',shallow';
        var text = '/command/' + 'monitor' + "/" + params.language + "/-/" + limit + '?format=rid,type,version' + shallow + ',class,graph';
        if (params.text) {
            var query = params.text.trim();
                             console.log(query);
            console.log(text);
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
    resource.getPropertyTableFromResults =  function (results) {
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
    return resource;
});




