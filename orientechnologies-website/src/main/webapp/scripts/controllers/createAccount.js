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
