'use strict';
angular.module('webappApp')
  .controller('IssueCtrl', function ($scope, Organization) {


    $scope.issues = Organization.all('issues').getList().$object;

  });

angular.module('webappApp')
  .controller('IssueNewCtrl', function ($scope, Organization, Repo, $location) {


    $scope.save = function () {
      Repo.one($scope.repo.name).all("issues").post($scope.issue).then(function (data) {
        $location.path("/issues/" + $scope.repo.name + "@" + data.uuid);
      });
    }
    Organization.all("repos").getList().then(function (data) {
      $scope.repositories = data.plain();
    })

    $scope.$watch("repo", function (val) {
      if (val) {
        Repo.one(val.name).all("teams").getList().then(function (data) {
          $scope.assignees = data.plain();
        })
        Repo.one(val.name).all("milestones").getList().then(function (data) {
          $scope.milestones = data.plain();
        });
      }
    });
  });
angular.module('webappApp')
  .controller('IssueEditCtrl', function ($scope, $routeParams, Repo) {

    var id = $routeParams.id.split("@");

    var repo = id[0];
    var number = id[1];

    Repo.one(repo).all("issues").one(number).get().then(function (data) {
      $scope.issue = data.plain();
    });


    $scope.comments = Repo.one(repo).all("issues").one(number).all("events").getList().$object;

    $scope.comment = function () {

      Repo.one(repo).all("issues").one(number).all("comments").post($scope.newComment).then(function (data) {
        $scope.comments.push(data.plain());
        $scope.newComment.body = "";
      });
    }

  });

