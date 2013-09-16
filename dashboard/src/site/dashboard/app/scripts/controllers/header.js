'use strict';

angular.module('MonitorApp')
    .controller('HeaderController', function ($scope, Monitor) {


        $scope.login = function () {
            Monitor.connect($scope.username, $scope.password, function (data) {

            }, function (data) {

            });
        }
    });