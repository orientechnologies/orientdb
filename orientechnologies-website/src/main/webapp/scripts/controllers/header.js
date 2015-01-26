'use strict';
angular.module('webappApp')
  .controller('HeaderCtrl', function ($scope, User, BreadCrumb) {

    $scope.menuClass = "";
    $scope.breadCrumb = BreadCrumb;
    User.whoami().then(function (user) {
      $scope.user = user;
      $scope.member = User.isMember(ORGANIZATION);
      $scope.isClient = User.isClient(ORGANIZATION);

    })
    $scope.toggleMenu = function () {
      $scope.menuClass = $scope.menuClass == "" ? "show-menu" : "";
    }


  });
