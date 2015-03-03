'use strict';
angular.module('webappApp')
  .controller('HeaderCtrl', function ($scope, User, BreadCrumb, $location, $rootScope) {

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

    $rootScope.$on('$routeChangeSuccess', function (scope, next, current) {

      if (!$scope.user) {
        $scope.$watch('user', function (user) {
          if (user && !user.confirmed) {
            if (!User.isMember(ORGANIZATION) && User.isClient(ORGANIZATION)) {
              $location.path('/users/' + user.name)
            }
          }
        })
      } else {
        if (!$scope.user.confirmed) {
          if (!User.isMember(ORGANIZATION) && User.isClient(ORGANIZATION)) {
            $location.path('/users/' + $scope.user.name)
          }
        }
      }
    })

  });
