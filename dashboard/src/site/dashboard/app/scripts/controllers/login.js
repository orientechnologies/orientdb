'use strict';

angular.module('MonitorApp')
    .controller('LoginController', function ($scope, Monitor) {
        $scope.awesomeThings = [
            'HTML5 Boilerplate',
            'AngularJS',
            'Karma'
        ];

        $scope.login = function () {
            Monitor.connect($scope.username, $scope.password, function (data) {

            }, function (data) {

            });
        }
    });