'use strict';

/**
 * @ngdoc function
 * @name webappApp.controller:MainCtrl
 * @description
 * # MainCtrl
 * Controller of the webappApp
 */
angular.module('webappApp')
  .controller('MainCtrl', function ($scope, Organization, User) {


    User.whoami().then(function (data) {
      $scope.member = data;
      if (User.isMember(ORGANIZATION)) {
        $scope.query = 'is:open ' + 'assignee:' + $scope.member.name + " sort:priority-desc";
      } else if (User.isClient(ORGANIZATION)) {
        var client = User.getClient(ORGANIZATION);
        $scope.query = 'is:open client:\"' + client.name + "\" sort:priority-desc";
      } else {
        $scope.query = 'is:open ';
      }
      Organization.all('issues').customGET("", {q: $scope.query, page: $scope.page}).then(function (data) {
        $scope.issues = data.content;
      });
    });


  });
