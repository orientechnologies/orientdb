/**
 * Created with JetBrains WebStorm.
 * User: erisa
 * Date: 26/08/13
 * Time: 13.55
 * To change this template use File | Settings | File Templates.
 */


var spinner = angular.module('message.services', []);

spinner.factory('Message', function (Monitor, $http, $resource) {

    var resource = $resource(API + 'database/:database');
    resource.getUnread = function (callback) {
        var query = "select * from Message where status = 'received' or status is null";
        $http.post(API + 'command/monitor/sql/-/-1', query).success(function (data) {
            callback(data);
        });
    }

    resource.installMsg = function (msg, callback) {
        $http.post(API + 'message/monitor/execute', msg).success(function (data) {
            callback(data);
        });
    }
    resource.checkUpdates = function () {
        $http.get(API + 'message/monitor/update').success(function (data) {
        });
    }
    return resource;
});