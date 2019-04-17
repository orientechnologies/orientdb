"use strict";

angular
  .module("webappApp")
  .controller("CreateAccountCtrl", function(
    $scope,
    User,
    Notification,
    $location
  ) {
    $scope.createAccount = function() {
      User.create($scope.user)
        .then(function(user) {
          Notification.success(
            "Account created. It will be activated soon or write to support@orientdb.com for activation",
            10000
          );
          $location.path("login");
        })
        .catch(function(err) {
          var text = err.data.message || err.data.statusText;
          Notification.error(text, 5000);
        });
    };
  });

angular
  .module("webappApp")
  .controller("RestorePasswordCtrl", function(
    $scope,
    User,
    Notification,
    $location
  ) {
    $scope.restorePassword = function() {
      User.resetPassword($scope.user)
        .then(function(user) {
          Notification.success(
            "Check your inbox for password recovery instructions.",
            10000
          );
          $location.path("/login");
        })
        .catch(function(err) {
          var text = err.data.message || err.data.statusText;
          Notification.error(text, 5000);
        });
    };
  });

angular
  .module("webappApp")
  .controller("ValidateTokenCtrl", function(
    $scope,
    User,
    $routeParams,
    AccessToken,
    $window,
    $location,
    Notification,
    $timeout
  ) {
    if ($routeParams.token) {
      User.validateToken($routeParams.token)
        .then(function(data) {
          AccessToken.set(data.token);
          $location.path("/change-password/" + $routeParams.token);
          
          $timeout(function() {
            $window.location.reload();
          },100)
          
        })
        .catch(function(err) {
          Notification.error(err.data.message, 5000);
          $location.path("/login");
        });
    }
  });

angular
  .module("webappApp")
  .controller("ChangePasswordCtrl", function(
    $scope,
    User,
    Notification,
    $routeParams,
    AccessToken,
    $window,
    $location
  ) {
    $scope.user = {};
    $scope.changePassword = function() {
      var promise;

      if ($routeParams.token) {
        promise = User.restorePassword($scope.user);
      } else {
        promise = User.changePassword($scope.user);
      }
      promise
        .then(function(data) {
          Notification.success("Password updated");
          $location.path("/");
        })
        .catch(function(err) {
          Notification.error(err.data.message, 5000);
        });
    };
    if ($routeParams.token) {
      $scope.user.token = $routeParams.token;
    }
  });
