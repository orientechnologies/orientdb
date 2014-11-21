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
  .controller('IssueEditCtrl', function ($scope, $routeParams, Repo, $popover, $route) {

    var id = $routeParams.id.split("@");

    var repo = id[0];
    var number = id[1];

    Repo.one(repo).all("issues").one(number).get().then(function (data) {
      $scope.issue = data.plain();
    });


    $scope.getEventTpl = function (e) {
      return 'views/issues/events/' + e + ".html";
    }
    function refreshEvents() {
      $scope.comments = Repo.one(repo).all("issues").one(number).all("events").getList().$object;
    }

    refreshEvents();


    $scope.labels = Repo.one(repo).all("labels").getList().$object;

    Repo.one(repo).all("milestones").getList().then(function (data) {
      $scope.versions = data.plain();
      $scope.milestones = data.plain();
    });
    Repo.one(repo).all("teams").getList().then(function (data) {
      $scope.assignees = data.plain();
    })
    $scope.sync = function () {
      Repo.one(repo).all("issues").one(number).all("sync").post().then(function (data) {
        $route.reload();
      });
    }
    $scope.comment = function () {

      Repo.one(repo).all("issues").one(number).all("comments").post($scope.newComment).then(function (data) {
        $scope.comments.push(data.plain());
        $scope.newComment.body = "";
      });
    }

    $scope.close = function () {
      Repo.one(repo).all("issues").one(number).patch({state: "closed"}).then(function (data) {
        $scope.issue.state = "closed";
        refreshEvents();
      });
    }
    $scope.reopen = function () {
      Repo.one(repo).all("issues").one(number).patch({state: "open"}).then(function (data) {
        $scope.issue.state = "open";
        refreshEvents();
      });
    }
    // CHANGE LABEL EVENT
    $scope.$on("label:added", function (e, label) {

      Repo.one(repo).all("issues").one(number).all("labels").post([label.name]).then(function (data) {
        $scope.issue.labels.push(label);
        refreshEvents();
      });


    })
    $scope.$on("label:removed", function (e, label) {


      Repo.one(repo).all("issues").one(number).one("labels", label.name).remove().then(function () {
        var idx = $scope.issue.labels.indexOf(label);
        $scope.issue.labels.splice(idx, 1);
        refreshEvents();
      });
    })

    // CHANGE VERSION EVENT

    $scope.$on("version:changed", function (e, version) {

      Repo.one(repo).all("issues").one(number).patch({version: version.number}).then(function (data) {
        $scope.issue.version = version;
        refreshEvents();
      })


    });
    // CHANGE MILESTONE EVENT
    $scope.$on("milestone:changed", function (e, milestone) {

      Repo.one(repo).all("issues").one(number).patch({milestone: milestone.number}).then(function (data) {
        $scope.issue.milestone = milestone;
        refreshEvents();
      })


    });

    $scope.$on("assignee:changed", function (e, assignee) {
      Repo.one(repo).all("issues").one(number).patch({assignee: assignee.name}).then(function (data) {
        $scope.issue.assignee = assignee;
        refreshEvents();
      })


    });
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
    $scope.isMilestoneSelected = function (milestone) {
      return $scope.issue.milestone ? milestone.number == $scope.issue.milestone.number : false;
    }
    $scope.toggleMilestone = function (milestone) {
      if (!$scope.isMilestoneSelected(milestone)) {
        $scope.$emit("milestone:changed", milestone);
        $scope.$hide();
      }
    }

  });
angular.module('webappApp')
  .controller('ChangeVersionCtrl', function ($scope, $routeParams, Repo, $popover) {

    $scope.isVersionSelected = function (version) {
      return $scope.issue.version ? version.number == $scope.issue.version.number : false;
    }
    $scope.toggleVersion = function (version) {
      if (!$scope.isVersionSelected(version)) {
        $scope.$emit("version:changed", version);
        $scope.$hide();
      }
    }
  });
angular.module('webappApp')
  .controller('ChangeAssigneeCtrl', function ($scope, $routeParams, Repo, $popover) {
    $scope.isAssigneeSelected = function (assignee) {
      return $scope.issue.assignee ? assignee.name == $scope.issue.assignee.name : false;
    }
    $scope.toggleAssignee = function (assignee) {
      if (!$scope.isAssigneeSelected(assignee)) {
        $scope.$emit("assignee:changed", assignee);
        $scope.$hide();
      }
    }

  });
