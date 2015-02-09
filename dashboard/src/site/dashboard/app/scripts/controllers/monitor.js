'use strict';

angular.module('MonitorApp')
  .controller('DashboardController', function ($scope, $location, $timeout, $modal, $q, $odialog, Monitor, Server, Notification, Settings, StickyNotification) {


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
      var modalPromise = $modal({
        template: 'views/settings/newModal.html',
        persist: true,
        show: false,
        backdrop: 'static',
        scope: modalScope
      });

      $q.when(modalPromise).then(function (modalEl) {
        modalEl.modal('show');
      });
    }
    $scope.addCluster = function () {

      var modalScope = $scope.$new(true)
      modalScope.refresh = $scope.refresh;
      var modalPromise = $modal({
        template: 'views/cluster/newCluster.html',
        persist: true,
        show: false,
        backdrop: 'static',
        scope: modalScope
      });

      $q.when(modalPromise).then(function (modalEl) {
        modalEl.modal('show');
      });
    }
    $scope.refreshConfig = function () {
      Settings.get(function (data) {
        if (data.result.length > 0) {
          $scope.config = data.result[0];
        }
      });
    }
    $scope.refresh = function () {
      Monitor.getServers(function (data) {
        $scope.servers = data.result;
        $scope.healthData = new Array;
        $scope.servers.forEach(function (elem, idx, arr) {
          $scope.healthData.push({label: elem.name, value: 100});
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
      var modalPromise = $modal({
        template: 'views/settings/editModal.html',
        persist: true,
        show: false,
        backdrop: 'static',
        scope: modalScope
      });

      $q.when(modalPromise).then(function (modalEl) {
        modalEl.modal('show');
      });
    }
    $scope.removeConfig = function (metr) {
      var idx = $scope.config["metrics"].indexOf(metr);
      $scope.config["metrics"].splice(idx, 1);
      $scope.saveConfigAndRefresh();

    }

    $scope.$watch("config.grid", function (data) {
      $scope.$emit("disposition:changed");
    });
    $scope.saveConfigAndRefresh = function () {
      Settings.put($scope.config, function (data) {
        $scope.refreshConfig();
      });
    }
    $scope.editCluster = function (cluster) {

      var modalScope = $scope.$new(true);
      modalScope.cluster = cluster;
      var modalPromise = $modal({
        template: 'views/cluster/editCluster.html',
        persist: true,
        show: false,
        backdrop: 'static',
        scope: modalScope
      });

      $q.when(modalPromise).then(function (modalEl) {
        modalEl.modal('show');
      });
    }
    $scope.enableServer = function (server) {
      Server.connect(server).then(function (data) {
        server.true = true;
      });
    }
    $scope.disableServer = function (server) {

      Server.disconnect(server).then(function (data) {
        server.attached = false;
      });
    }
    $scope.deleteServer = function (server) {

      $odialog.confirm({
        title: 'Warning!',
        body: 'You are removing Server ' + server.name + '. Are you sure?',
        success: function () {
          Server.delete(server.name, function (data) {
            $scope.refresh();
            $scope.refreshConfig();
          });
        }
      });

    }
    $scope.setOver = function (chart) {
      $scope.over = chart;
    }
    $scope.isOver = function (chart) {


      return $scope.over == chart;
    }
    $scope.refreshConfig();
    $scope.refresh();
  });

