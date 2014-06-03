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
module.controller('ClusterMainController', function ($scope, $i18n, Cluster, $modal, $q, Server) {


    Cluster.getAll().then(function (data) {
        $scope.clusters = data;
        if ($scope.clusters.length > 0) {
            $scope.cluster = $scope.clusters[0];
        }
    });


    $scope.db = false;
    $scope.editCluster = function () {

        if ($scope.cluster) {
            var modalScope = $scope.$new(true);
            modalScope.cluster = $scope.cluster;
            var modalPromise = $modal({template: 'views/cluster/editCluster.html', persist: true, show: false, backdrop: 'static', scope: modalScope});

            $q.when(modalPromise).then(function (modalEl) {
                modalEl.modal('show');
            });
        }
    }
    $scope.$watch("cluster", function (data) {

        if (data) {
            Cluster.getServers(data).then(function (servers) {
                $scope.servers = servers;
                $scope.nodes = new Array;
                $scope.servers.forEach(function (s, idx, arr) {
                    Server.findDatabasesOnSnapshot(s.name, function (data) {
                        s.databases = [];
                        data.forEach(function (db, idx, arr) {
                            s.databases.push(db);
                        });
                        $scope.nodes = $scope.nodes.concat(s);
                    });
                })

            });
        }
    });

    $scope.isInConfig = function (cluster, node) {
        return ($scope.dbConfig && $scope.dbConfig.config[0].clusters[cluster]) ? $scope.dbConfig.config[0].clusters[cluster].servers.indexOf(node) : false;
    }
    $scope.$on("dbselected", function (event, data) {
        if (data.el.db) {
            var db = data.el.name;
            Cluster.getClusterDbInfo($scope.cluster.name, data.el.name).then(function (data) {
                $scope.dbConfig = {name: db, config: data.result }
                var clusters = [];
                var arr = Object.keys($scope.dbConfig.config[0].clusters);
                arr.forEach(function (v) {
                    if (v.indexOf("@") != 0) {
                        clusters.push({name: v});
                    }
                });
                $scope.clusters = clusters;
                $scope.db = true;
            });
        }
    });
})
;

