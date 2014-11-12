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
  .controller('IssueEditCtrl', function ($scope, $routeParams, Repo, $popover) {

    var id = $routeParams.id.split("@");

    var repo = id[0];
    var number = id[1];

    Repo.one(repo).all("issues").one(number).get().then(function (data) {
      $scope.issue = data.plain();
    });


    $scope.comments = Repo.one(repo).all("issues").one(number).all("events").getList().$object;

    $scope.labels = Repo.one(repo).all("labels").getList().$object;

    Repo.one(repo).all("milestones").getList().then(function (data) {
      $scope.versions = data.plain();
      $scope.milestones = data.plain();
    });
    $scope.comment = function () {

      Repo.one(repo).all("issues").one(number).all("comments").post($scope.newComment).then(function (data) {
        $scope.comments.push(data.plain());
        $scope.newComment.body = "";
      });
    }

    // CHANGE LABEL EVENT
    $scope.$on("label:added", function (e, label) {

      Repo.one(repo).all("issues").one(number).all("labels").post([label.name]).then(function (data) {
        $scope.issue.labels.push(label);
        $scope.comments = Repo.one(repo).all("issues").one(number).all("events").getList().$object;
      });


    })
    $scope.$on("label:removed", function (e, label) {


      Repo.one(repo).all("issues").one(number).one("labels", label.name).remove().then(function () {
        var idx = $scope.issue.labels.indexOf(label);
        $scope.issue.labels.splice(idx, 1);
        $scope.comments = Repo.one(repo).all("issues").one(number).all("events").getList().$object;
      });
    })

    // CHANGE VERSION EVENT

    $scope.$on("version:changed", function (e, version) {

      $scope.issue.version = version;

    })
  });

angular.module('webappApp')
  .controller('ChangeLabelCtrl', function ($scope, $routeParams, Repo, $popover) {

    $scope.isLabeled = function (label) {

      for (var l in $scope.issue.labels) {
        if ($scope.issue.labels[l].name == label.name) {
          return true;
        }
      }
      return false;
    }
    $scope.toggleLabel = function (label) {
      if (!$scope.isLabeled(label)) {
        $scope.$emit("label:added", label);
      } else {
        $scope.$emit("label:removed", label);
      }
    }

  });

angular.module('webappApp')
  .controller('ChangeMilestoneCtrl', function ($scope, $routeParams, Repo, $popover) {


  });
angular.module('webappApp')
  .controller('ChangeVersionCtrl', function ($scope, $routeParams, Repo, $popover) {

    $scope.isVersionSelected = function (version) {
      return version.number == $scope.issue.version.number;
    }
    $scope.toggleVersion = function (version) {
      if (!$scope.isVersionSelected(version)) {
        $scope.$emit("version:changed", version);
      }
    }
  });

