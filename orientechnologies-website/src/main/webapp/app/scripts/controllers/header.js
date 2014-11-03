'use strict';
angular.module('webappApp')
  .controller('HeaderCtrl', function ($scope) {

    $scope.menuClass = "";

    $scope.toggleMenu = function () {
      $scope.menuClass = $scope.menuClass == "" ? "show-menu" : "";
    }

  });
