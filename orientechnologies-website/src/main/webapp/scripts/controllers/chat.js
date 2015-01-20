'use strict';

/**
 * @ngdoc function
 * @name webappApp.controller:AboutCtrl
 * @description
 * # AboutCtrl
 * Controller of the webappApp
 */
angular.module('webappApp')
  .controller('ChatCtrl', function ($scope, Organization, $routeParams,$route) {

    $scope.isNew = false;
    $scope.placeholder = "Click here to type a message.";
    $scope.clientId = $routeParams.id;
    Organization.all("clients").getList().then(function (data) {
      $scope.clients = data.plain();
      if ($scope.clients.length > 1) {
        if (!$scope.clientId) {
          $scope.clientId = $scope.clients[0].clientId.toString();
          $scope.client = $scope.clients[0];
        } else {
          $scope.clients.forEach(function (c) {
            if (c.clientId == $scope.clientId) {
              $scope.client = c;
            }
          })
        }
      }
      if ($scope.clientId) {
        Organization.all("clients").one($scope.clientId).all("room").getList().then(function (data) {
          $scope.messages = data.plain();
          $scope.messages.reverse();
        }).catch(function (e) {
          if (e.status == 400 && e.data) {
            $scope.isNew = true;
            //var jacked = humane.create({baseCls: 'humane-jackedup', addnCls: 'humane-jackedup-error'})
            //jacked.log(e.data.message)
          }
        });
      }
    })

    $scope.createChat = function () {
      Organization.all("clients").one($scope.clientId).all("room").post().then(function (data) {
        var jacked = humane.create({baseCls: 'humane-jackedup', addnCls: 'humane-jackedup-success'})
        jacked.log("Room created");
        $route.reload();
      });
    }
    $scope.sendMessage = function () {
      Organization.all("clients").one($scope.clientId).all("room").patch({body: $scope.current}).then(function (data) {
        $scope.current = null;
        $scope.messages.push(data);
      })
    }
  });
