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
  .controller('ClientEditCtrl', function ($scope, Organization, $routeParams, $location, Notification) {


    $scope.contractEditing = false;
    Organization.all("clients").one($routeParams.id).get().then(function (data) {
      $scope.client = data.plain();
    }).catch(function (error, status) {
      if (error.status == 404) {
        $location.path("/");
      }
    })
    Organization.all("clients").one($routeParams.id).all("members").getList().then(function (data) {
      $scope.members = data.plain();
    })
    Organization.all("clients").one($routeParams.id).all("environments").getList().then(function (data) {
      $scope.environments = data.plain();
    })

    Organization.all("clients").one($routeParams.id).all("contracts").getList().then(function (data) {
      $scope.contracts = data.plain();
    })
    Organization.all("contracts").getList().then(function (data) {
      $scope.contractsTypes = data.plain();
    })
    $scope.save = function () {
      Organization.all("clients").one($routeParams.id).patch($scope.client).then(function (data) {
        Notification.success("Client updated successfully.");
      })
    }

    $scope.saveContract = function () {

      Organization.all("clients").one($routeParams.id).all("contracts").post($scope.selectedContract).then(function (data) {
        $scope.contracts.push(data.plain());
        $scope.selectedContract = null;
        $scope.cancelContract();
      })
    }
    $scope.addContract = function () {
      $scope.contractEditing = true;
    }
    $scope.cancelContract = function () {
      $scope.contractEditing = false;
    }
    $scope.createChat = function () {
      Organization.all("clients").one($routeParams.id).all("room").post().then(function (data) {
        console.log("chat created")
      });
    }
    $scope.addMember = function () {
      Organization.all("clients").one($routeParams.id).all("members").one($scope.newMember).post().then(function (data) {
        $scope.members.push(data);
        Notification.success("Member added correctly.");
      }).catch(function (err) {
        Notification.error("Error on removing member :" + err);
      });
    }
    $scope.removeMember = function (member) {
      Organization.all("clients").one($routeParams.id).all("members").one(member.name).remove().then(function (data) {
        var idx = $scope.members.indexOf(member);
        $scope.members.splice(idx, 1);
        Notification.success("Member removed correctly.");
      }).catch(function (err) {
        Notification.error("Error on removing member :" + err);
      });
    }
  });
