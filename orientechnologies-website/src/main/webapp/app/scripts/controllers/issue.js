'use strict';
angular.module('webappApp')
  .controller('IssueCtrl', function ($scope, Organization) {


    $scope.issues = Organization.all('issues').getList().$object;

  });

angular.module('webappApp')
  .controller('IssueNewCtrl', function ($scope, Organization, Repo) {


    $scope.repositories = Organization.all("repos").getList().$object;

    $scope.$watch("repo", function (val) {
      if (val) {
        $scope.assignees = Repo.one(val.name).all("teams").getList().$object;
        $scope.milestones = Repo.one(val.name).all("milestones").getList().$object;
      }
    });
  });
angular.module('webappApp')
  .controller('IssueEditCtrl', function ($scope, $routeParams, Repo) {

    var id = $routeParams.id.split("@");

    var repo = id[0];
    var number = id[1];

    $scope.issue = Repo.one(repo).all("issues").one(number).get().$object;


    $scope.comments = Repo.one(repo).all("issues").one(number).all("events").getList().$object;

  });

