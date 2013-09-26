/**
 * Created with JetBrains WebStorm.
 * User: marco
 * Date: 24/09/13
 * Time: 12.07
 * To change this template use File | Settings | File Templates.
 */
var biconsole = angular.module('workbench-logs.services', ['ngResource']);

biconsole.factory('CommandLogApi', function ($http, $resource) {

    var resource = $resource($http);
    console.log(resource);
    resource.getLogs = function (params, callback) {

//        params.file
//        params.dateFrom
//        params.dateTo
//        params.hourFrom
//        params.hourTo
//        params.type
//        params.info
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
        var server = '&name='+params.server;

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

    resource.getListFiles = function (callback) {


        $http.get('/log/files').success(function (data) {
            callback(data);
        }).error(function (data) {
            })
    }

    return resource;
});




