'use strict';
angular.module('webappApp')
  .controller('HeaderCtrl', function ($scope, User) {

    $scope.menuClass = "";

    User.whoami().then(function (user) {
      $scope.user = user;
      $scope.member = User.isMember(ORGANIZATION);
    })
    $scope.toggleMenu = function () {
      $scope.menuClass = $scope.menuClass == "" ? "show-menu" : "";
    }


  });
