'use strict';

var module = angular.module('MonitorApp');
module.controller('ClusterNewController', function ($scope, Cluster) {

    $scope.cluster = new Object;
    $scope.cluster['@rid'] = '#-1:-1';
    $scope.cluster['@class'] = 'Cluster';
    $scope.cluster.multicast = new Object();
    $scope.cluster.tcp = new Object();
    $scope.cluster.port = "2434";
    $scope.cluster.portIncrement = true;
    $scope.cluster.multicast.enabled = true;
    $scope.cluster.tcp.enabled = false;
    $scope.cluster.tcp.members = [];
    $scope.cluster.multicast.group = "235.1.1.1";
    $scope.cluster.multicast.port = 2434;


    $scope.save = function () {
        Cluster.saveCluster($scope.cluster).then(function (data) {

        })
    }

});
module.controller('ClusterEditController', function ($scope, Cluster) {


    $scope.save = function () {
        Cluster.saveCluster($scope.cluster).then(function (data) {

        })
    }

});
module.controller('ClusterChangeController', function ($scope, Cluster) {

    $scope.dimension = Object.keys($scope.dirty);


});
module.controller('ClusterMainController', function ($scope, $i18n, Cluster, $modal, $q, Server, $odialog, $routeParams, $location, $filter, Spinner) {


    if ($routeParams.db) {
        $scope.dbselected = {el: { db: true}, name: $routeParams.db};
    }

    Cluster.getAll().then(function (data) {
        $scope.nodeClusters = data;
        if ($scope.nodeClusters.length > 0) {
            $scope.cluster = $scope.nodeClusters[0];
            $location.path("dashboard/cluster/" + $scope.cluster.name, false);
        }
    });

    $scope.dirty = {};
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
    $scope.removeCluster = function () {

        if ($scope.cluster) {
            $odialog.confirm({
                title: 'Warning!',
                body: 'You are removing Cluster ' + $scope.cluster.name + '. Are you sure?',
                success: function () {
                    Cluster.delete($scope.cluster.name, function (data) {
                        var idx = $scope.nodeClusters.indexOf($scope.cluster);
                        $scope.nodeClusters.splice(idx, 1);
                    });
                }
            });
        }
    }
    $scope.$watch("cluster", function (data) {

        if (data) {
            Cluster.getServers(data).then(function (servers) {
                $scope.servers = servers;
                $scope.nodes = new Array;
                $scope.servers.forEach(function (s, idx, arr) {
                    if (s.status == 'ONLINE') {
                        Server.findDatabases(s.name, function (data) {
                            s.databases = [];
                            data.forEach(function (db, idx, arr) {
                                s.databases.push(db);
                            });
                            $scope.nodes = $scope.nodes.concat(s);
                        });
                    } else {
                        Server.findDatabasesOnSnapshot(s.name, function (data) {
                            s.databases = [];
                            data.forEach(function (db, idx, arr) {
                                s.databases.push(db);
                            });
                            $scope.nodes = $scope.nodes.concat(s);
                        });
                    }

                });
                $scope.servers.push({name: "<NEW_NODE>"})
            });
        }
    });

    $scope.isInConfig = function (cluster, node) {
        return ($scope.dbConfig && $scope.dbConfig.config.clusters[cluster]) ? $scope.dbConfig.config.clusters[cluster].servers.indexOf(node) : false;
    }
    $scope.saveConfig = function () {

        var modalScope = $scope.$new(true);
        modalScope.dirty = $scope.dirty;
        modalScope.ok = function () {
            var meta = $scope.dbConfig.config.metadata;
            delete $scope.dbConfig.config.metadata;
            Cluster.saveClusterDbInfo($scope.cluster.name, $scope.dbConfig.name, $scope.dbConfig.config).then(function (data) {
                $scope.dbConfig.config.metadata = meta;
            });
        }
        modalScope.cluster = $scope.cluster;
        var modalPromise = $modal({template: 'views/cluster/changeCluster.html', persist: true, show: false, backdrop: 'static', scope: modalScope});

        $q.when(modalPromise).then(function (modalEl) {
            modalEl.modal('show');
        });


    }
    $scope.addCluster = function () {
        $scope.clusters.push({name: ""});
    }
    $scope.isSelectedCluster = function (c, p) {
        return c.name == p.name;
    }
    $scope.setDirtyCluster = function (cluster, p) {
        cluster.name = p;
        $scope.matrix[p] = [];
    }
    $scope.changeServerConfig = function (c, s, bool) {
        $scope.matrix[c][s] = bool;
        if (!$scope.dbConfig.config.clusters[c]) {
            $scope.dbConfig.config.clusters[c] = { servers: []}
        }
        if (!$scope.dirty[c]) {
            $scope.dirty[c] = {};
        }
        $scope.dirty[c][s] = bool;
        if (bool) {

            $scope.dbConfig.config.clusters[c].servers.push(s);
        } else {
            var idx = $scope.dbConfig.config.clusters[c].servers.indexOf(s);
            $scope.dbConfig.config.clusters[c].servers.splice(idx, 1);
        }

    }
    $scope.connect = function () {
        Spinner.start();
        Cluster.connect($scope.cluster.name).then(function () {
            $scope.cluster.status = 'ONLINE';
            Spinner.stopSpinner();
        });
    }
    $scope.disconnect = function () {
        Spinner.start();
        Cluster.disconnect($scope.cluster.name).then(function () {
            $scope.cluster.status = 'OFFLINE';
            Spinner.stopSpinner();
        });
    }
    $scope.deployDb = function (s) {
        $odialog.confirm({
            title: 'Warning!',
            body: "You are deploying the DB " + $scope.currentDb + " to the server  " + s.name + ". This operation will overwrite the existing DB, if any. Are you sure?",
            success: function () {
                Cluster.deployDb($scope.cluster.name, s.name, $scope.currentDb).then(function (data) {

                });
            }
        });
    }
    $scope.$on("dbselected", function (event, data) {


        if (data.el.db) {
            $scope.currentDb = data.el.name;
            var db = data.el.name;
            Cluster.getClusterDbInfo($scope.cluster.name, data.el.name).then(function (data) {
                $scope.dbConfig = {name: db, config: data.result[0], servers: $scope.servers }
                $scope.phisicalCluster = $scope.dbConfig.config.metadata.clusters;
                $scope.phisicalCluster.push({name: '*'});
                var clusters = [];
                var arr = Object.keys($scope.dbConfig.config.clusters);
                arr.forEach(function (v) {
                    if (v.indexOf("@") != 0) {
                        clusters.push({name: v});
                    }
                });

                $scope.clusters = clusters;
                $scope.db = true;
                $scope.matrix = [];
                $scope.clusters.forEach(function (val) {
                    $scope.matrix[val.name] = [];
                    if ($scope.dbConfig.config.clusters[val.name]) {
                        $scope.dbConfig.config.clusters[val.name].servers.forEach(function (s) {
                            $scope.matrix[val.name][s] = true;
                        });
                    }
                });

                $location.path("dashboard/cluster/" + $scope.cluster.name + "/" + db, false);
            });
        } else {
            var path = "dashboard/general/" + $filter('nopound')(data.el['@rid']);
            $scope.$apply(function () {
                $location.path(path);
            });
        }
    });
})
;

