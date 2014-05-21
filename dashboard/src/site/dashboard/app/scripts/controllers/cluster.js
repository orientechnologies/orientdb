'use strict';

var module = angular.module('MonitorApp');
module.controller('ClusterNewController', function ($scope, Cluster) {

    $scope.cluster = new Object;
    $scope.cluster['@rid'] = '#-1:-1';
    $scope.cluster['@class'] = 'Cluster';
    $scope.cluster.multicast = new Object();
    $scope.cluster.port = "2434";
    $scope.cluster.portIncrement = true;
    $scope.cluster.multicast.enabled = true;
    $scope.cluster.multicast.group = "235.1.1.1";
    $scope.cluster.multicast.port = 2434;


    $scope.save = function () {
        Cluster.saveCluster($scope.cluster).then(function (data) {
            console.log(data);
        })
    }

});
module.controller('ClusterEditController', function ($scope, Cluster) {


    $scope.save = function () {
        Cluster.saveCluster($scope.cluster).then(function (data) {
            console.log(data);
        })
    }

});

