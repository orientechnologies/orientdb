'use strict';

angular.module('MonitorApp')
    .controller('LoginController', function ($scope, Monitor,Login) {

        $scope.login = function () {

        	if (!$scope.username) $scope.username = $('#input_username').val();
        	if (!$scope.password) $scope.password = $('#input_password').val();
            Login.login($scope.username, $scope.password,function(){},function(){$scope.errorMsg = undefined;$scope.errorMsg ='Username or password invalid.'});

        }
    });