'use strict';

var module = angular.module('MonitorApp');
module.controller('ServersController', function ($scope, $location, $routeParams, Monitor) {

    Monitor.serverStatus(function (data) {
        $scope.servers = data.result;
    })
});

function ServerBaseController($scope, $location, $routeParams, Monitor) {

    $scope.save = function () {
        Monitor.updateServer($scope.server, function (data) {
              $scope.refresh();
        });
    }
};
module.controller('ServerNewController', function ($scope, $location, $injector, $routeParams, Monitor,Server) {


    $injector.invoke(ServerBaseController, this, {$scope: $scope});
    $scope.server = new Object;
    $scope.server['@rid'] = '#-1:-1';
    $scope.server['@class'] = 'Server';

    $scope.test = function(){
         Server.isAlive($scope.server,function(data){
             $scope.testMsg = 'Connection is alive';
             $scope.testMsgClass = 'alert alert-success'
         }, function(data){
             $scope.testMsg = 'No connection';
             $scope.testMsgClass = 'alert alert-error'

         });
    }
});
module.controller('ServerEditController', function ($scope, $location, $injector, $routeParams, Monitor) {

    $injector.invoke(ServerBaseController, this, {$scope: $scope});
    Monitor.getServer($scope.serverID, function (data) {
        $scope.server = data;
    });

});