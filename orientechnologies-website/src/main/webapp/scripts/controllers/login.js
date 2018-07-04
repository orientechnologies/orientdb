"use strict";

angular
  .module("webappApp")
  .controller("LoginCtrl", function(
    $scope,
    User,
    Notification,
    AccessToken,
    $location,
    $window
  ) {
    $scope.login = function() {
      User.login($scope.user)
        .then(function(res) {
          AccessToken.set(res.token);
          $location.path("/");
          $window.location.reload();
        })
        .catch(function(err) {
          var text = err.data.message || err.data.statusText;
          Notification.error(text, 5000);
        });
    };
  });
