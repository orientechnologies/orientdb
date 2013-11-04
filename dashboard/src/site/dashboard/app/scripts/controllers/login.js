'use strict';

angular.module('MonitorApp')
    .controller('LoginController', function ($scope, Monitor,Login) {

        $scope.login = function () {

            Login.login($scope.username, $scope.password);

        }
    });