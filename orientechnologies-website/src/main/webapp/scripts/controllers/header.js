"use strict";
angular
  .module("webappApp")
  .controller("HeaderCtrl", function(
    $scope,
    User,
    BreadCrumb,
    $location,
    $route,
    $rootScope,
    AccessToken,
    $window,
    $timeout
  ) {
    $scope.menuClass = "";
    $scope.breadCrumb = BreadCrumb;
    User.whoami().then(function(user) {
      $scope.user = user;
      $scope.member = User.isMember(ORGANIZATION);
      $scope.isClient = User.isClient(ORGANIZATION);
    });

    $scope.closeMe = function() {
      $("#myNavmenu").offcanvas("toggle");
    };
    $scope.toggleMenu = function() {
      $scope.menuClass = $scope.menuClass == "" ? "show-menu" : "";
    };

    $scope.logout = function() {
      AccessToken.set(null);

      $timeout(function() {
        $window.location.reload();
      }, 100);
    };
    $rootScope.$on("$routeChangeSuccess", function(scope, next, current) {
      if (next.$$route.originalPath.indexOf("/rooms") != -1) {
        $scope.isChat = true;
      } else {
        $scope.isChat = false;
      }
      if (!$scope.user) {
        $scope.$watch("user", function(user) {
          if (user && !user.confirmed) {
            if (!User.isMember(ORGANIZATION) && User.isClient(ORGANIZATION)) {
              $location.path("/users/" + user.name);
            }
          }
        });
      } else {
        if (!$scope.user.confirmed) {
          if (!User.isMember(ORGANIZATION) && User.isClient(ORGANIZATION)) {
            $location.path("/users/" + $scope.user.name);
          }
        }
      }
    });
  });
