'use strict';

angular.module('MonitorApp')
    .controller('DashboardController', function ($scope, $location, $timeout, $modal, $q, $odialog, Monitor, Server, Notification, Settings) {


        $scope.chartHeight = 300;
        $scope.pollTime = 5000;


        var watcher;
        $scope.startWatching = function () {
            watcher = $timeout(function () {
                $scope.refresh();
                $scope.startWatching();
            }, $scope.pollTime);
        }
        $scope.stopWatching = function () {
            $timeout.cancel(watcher);
        }
        $scope.$on('$routeChangeStart', function () {
            $scope.stopWatching();
        });
        $scope.startWatching();
        $scope.addServer = function () {

            var modalScope = $scope.$new(true)
            modalScope.refresh = $scope.refresh;
            var modalPromise = $modal({template: 'views/settings/newModal.html', persist: true, show: false, backdrop: 'static', scope: modalScope});

            $q.when(modalPromise).then(function (modalEl) {
                modalEl.modal('show');
            });
        }
        $scope.refresh = function () {
            Monitor.getServers(function (data) {
                $scope.servers = data.result;
                $scope.healthData = new Array;
                $scope.servers.forEach(function (elem, idx, arr) {
                    $scope.healthData.push({ label: elem.name, value: 100});
                });
            });
            Notification.latest(function (data) {
                $scope.notifications = data.result;
            });
        }
        $scope.getGridClass = function (index, gridOpt) {
            var css = '';
            css += (index % gridOpt == 0) ? 'no-margin' : '';
            css += ' span' + 12 / gridOpt;
            return css;
        }
        $scope.getStatusLabel = function (status) {
            var label = 'label ';
            label += status == 'ONLINE' ? 'label-success' : 'label-important';
            return label;
        }
        $scope.editServer = function (rid) {

            var modalScope = $scope.$new(true);
            modalScope.refresh = $scope.refresh;
            modalScope.serverID = rid;
            var modalPromise = $modal({template: 'views/settings/editModal.html', persist: true, show: false, backdrop: 'static', scope: modalScope});

            $q.when(modalPromise).then(function (modalEl) {
                modalEl.modal('show');
            });
        }
        $scope.deleteServer = function (server) {

            $odialog.confirm({
                title: 'Warning!',
                body: 'You are removing Server ' + server.name + '. Are you sure?',
                success: function () {
                    Server.delete(server.name, function (data) {
                        $scope.refresh();
                    });
                }
            });

        }

        Settings.get(function (data) {
            if (data.result.length > 0) {
                $scope.config = data.result[0];
            }
        });
        $scope.refresh();
    });