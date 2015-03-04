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


    $scope.issue = {}
    User.whoami().then(function (data) {
      $scope.member = data;
      $scope.issue.assignee = data;
      if (User.isMember(ORGANIZATION)) {
        $scope.query = 'is:open ' + 'assignee:' + $scope.member.name + " sort:priority-desc";
      } else if (User.isClient(ORGANIZATION)) {
        var client = User.getClient(ORGANIZATION);
        $scope.query = 'is:open client:\"' + client.name + "\" sort:priority-desc";
      } else {
        $scope.query = 'is:open ';
      }
      Organization.all("members").getList().then(function (data) {
        $scope.assignees = data.plain();
      })
      Organization.all('issues').customGET("", {q: $scope.query, page: $scope.page}).then(function (data) {
        $scope.issues = data.content;
      });
      function loadBoard() {
        var assignee = $scope.issue.assignee ? $scope.issue.assignee.name : $scope.member.name;
        $scope.queryBacklog = 'is:open assignee:' + assignee + " !label:\"In Progress\" sort:priority-desc sort:createdAt-desc";
        Organization.all('board').all("issues").customGET("", {
          q: $scope.queryBacklog,
          page: $scope.page
        }).then(function (data) {
          $scope.backlogs = data.content;
        });
        $scope.queryProgress = 'is:open assignee:' + assignee + " label:\"In Progress\" sort:priority-desc sort:createdAt-desc";
        Organization.all('board').all("issues").customGET("", {
          q: $scope.queryProgress,
          page: $scope.page
        }).then(function (data) {
          $scope.inProgress = data.content;
        });
      }

      if (User.isMember(ORGANIZATION)) {
        loadBoard();
      }
      $scope.$on("assignee:changed", function (e, assignee) {
        if (assignee) {
          $scope.issue.assignee = assignee;
          loadBoard();
        }
      });
    });


  });
