/**
 * Created with JetBrains WebStorm.
 * User: erisa
 * Date: 02/09/13
 * Time: 18.41
 * To change this template use File | Settings | File Templates.
 */

var ctrl = angular.module('server.controller', []);
ctrl.controller("ServerController", ['$scope', '$routeParams', 'ServerApi', function ($scope, $routeParams, ServerApi) {

    $scope.active = $routeParams.tab || "conn";
    $scope.tabs = ["conn", "config", "pool", "storage"];
    ServerApi.getServerInfo(function (data) {
        $scope.connections = data.connections;
        $scope.properties = data.properties;
        $scope.storages = data.storages;
    });

    $scope.getTemplate = function (tab) {
        return 'views/server/' + tab + '.html';
    }

    $scope.killConnection = function (n) {
        ServerApi.killConnection(n.connectionId, function () {
            var index = $scope.connections.indexOf(n);
            $scope.connections.splice(index, 1);
        });
    }
    $scope.interruptConnection = function (n) {
        ServerApi.interruptConnection(n.connectionId, function () {
            var index = $scope.connections.indexOf(n);
            $scope.connections.splice(index, 1);
        });
    }
}]);


ctrl.controller("ServerStatusController", ['$scope', '$rootScope', function ($scope, $rootScope) {

    $scope.isDown = false;
    $scope.serverclass = 'hide';
    $rootScope.$on('server:down', function () {
        $scope.isDown = true;
    })
    $rootScope.$on('server:up', function () {
        $scope.isDown = false;
    })
}]);