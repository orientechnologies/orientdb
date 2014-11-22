'use strict';

/**
 * @ngdoc function
 * @name webappApp.controller:MainCtrl
 * @description
 * # MainCtrl
 * Controller of the webappApp
 */
angular.module('webappApp')
  .controller('ClientCtrl', function ($scope, Organization) {

    Organization.all("clients").getList().then(function (data) {
      $scope.clients = data.plain();
    })
  });

angular.module('webappApp')
  .controller('ClientNewCtrl', function ($scope, Organization, $location) {

    $scope.save = function () {
      Organization.all("clients").post($scope.client).then(function (data) {
        $location.path("/clients/" + $scope.client.clientId);
      })
    }
  });

angular.module('webappApp')
  .controller('ClientEditCtrl', function ($scope, Organization, $routeParams) {


    Organization.all("clients").one($routeParams.id).get().then(function (data) {
      $scope.client = data.plain();
    })
    Organization.all("clients").one($routeParams.id).all("members").getList().then(function (data) {
      $scope.members = data.plain();
    })
    $scope.save = function () {

    }
    $scope.addMember = function () {
      Organization.all("clients").one($routeParams.id).all("members").one($scope.newMember).post().then(function (data) {
        $scope.members.push(data);
      });
    }
  });
