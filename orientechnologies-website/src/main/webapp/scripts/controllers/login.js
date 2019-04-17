"use strict";

angular
  .module("webappApp")
  .controller("LoginCtrl", function(
    $scope,
    User,
    Notification,
    AccessToken,
    $location,
    $window,
    $timeout
  ) {


    $scope.login = function() {
      User.login($scope.user)
        .then(function(res) {
          AccessToken.set(res.token);
          $location.path("/");
        
          $timeout(function() {
            $window.location.reload();
          },100)
        })
        .catch(function(err) {
          var text = err.data.message || err.data.statusText;
          Notification.error(text, 5000);
        });
    };
  });
