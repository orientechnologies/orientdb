'use strict';

var login = angular.module('MonitorApp');
login.controller('LoginController', function ($scope, Monitor, Login) {

    $scope.login = function () {

        if (!$scope.username) $scope.username = $('#input_username').val();
        if (!$scope.password) $scope.password = $('#input_password').val();
        Login.login($scope.username, $scope.password, function () {
        }, function () {
            $scope.errorMsg = undefined;
            $scope.errorMsg = 'Username or password invalid.'
        });

    }
});

login.controller('ChangePasswordController', function ($scope, Users, Login, $location) {

    $scope.username = Login.username;
    Users.getWithUsername($scope.username, function (data) {
        $scope.user = data;
    });
    $scope.save = function () {

        $scope.user.password = $scope.newpassword;
        Users.savePasswd($scope.user, function (data) {
            Login.logout();
        });
    }
});