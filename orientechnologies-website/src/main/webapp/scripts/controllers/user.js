'use strict';

/**
 * @ngdoc function
 * @name webappApp.controller:AboutCtrl
 * @description
 * # AboutCtrl
 * Controller of the webappApp
 */
angular.module('webappApp')
  .controller('UserCtrl', function ($scope, User) {

    $scope.tabs = [{
      title: 'Environment',
      url: 'views/users/environment.html'
    }]

    $scope.currentTab = $scope.tabs[0].url;

    $scope.onClickTab = function (tab) {
      $scope.currentTab = tab.url;
    }

    $scope.isActiveTab = function (tabUrl) {
      return tabUrl == $scope.currentTab;
    }

    User.whoami().then(function (user) {
      $scope.user = user;
      if (User.isMember(ORGANIZATION)) {
        $scope.tabs.push({
          title: 'Organization',
          url: 'views/users/organization.html'
        })
      }
    });
  });

angular.module('webappApp')
  .controller('UserEnvCtrl', function ($scope, User, Repo) {


    $scope.environment = {}
    $scope.connections = ['plocal', 'remote', 'memory']
    Repo.one(DEFAULT_REPO).all("milestones").getList().then(function (data) {
      $scope.milestones = data.plain();
    });
    User.whoami().then(function (user) {
      $scope.user = user;
      User.environments().then(function (data) {
        $scope.environments = data.plain();
      })
    });

    $scope.newEnv = function () {
      $scope.environment = {};
      $scope.environment.distributed = false;
      // hardcoded to be able to lookup for Milestone from id
      $scope.environment.repoName = DEFAULT_REPO;
      $scope.isNew = true;
      $scope.editing = true;

    }
    $scope.cancelSave = function () {
      $scope.environment = {};
      $scope.currentEditing
      // hardcoded to be able to lookup for Milestone from id
      $scope.isNew = false;
      $scope.editing = false;

    }
    $scope.deleteEnv = function (env) {
      User.deleteEnvironment(env).then(function (data) {
        var idx = $scope.environments.indexOf(env);
        $scope.environments.splice(idx, 1);
      });
    }
    $scope.saveEnv = function () {

      if ($scope.isNew) {
        User.addEnvironment($scope.environment).then(function (data) {

          $scope.isNew = false;
          $scope.editing = false;
          $scope.environments.push(data);
        });
      } else {
        User.changeEnvironment($scope.environment).then(function (data) {
          $scope.isNew = false;
          $scope.editing = false;
          var idx = $scope.environments.indexOf($scope.currentEditing);
          $scope.environments[idx] = data;
          $scope.currentEditing = null
          $scope.environment = {}
        });
      }

    }
    $scope.editEnv = function (env) {
      $scope.environment = angular.copy(env);
      $scope.environment.repoName = DEFAULT_REPO;
      $scope.currentEditing = env;
      $scope.isNew = false;
      $scope.editing = true;
    }

  });

angular.module('webappApp')
  .controller('ChangeEnvironmentCtrl', function ($scope, User, Repo) {

    $scope.environment = angular.copy($scope.issue.environment)

    $scope.connections = ['plocal', 'remote', 'memory']
    Repo.one(DEFAULT_REPO).all("milestones").getList().then(function (data) {
      $scope.milestones = data.plain();
    });

    $scope.save = function () {
      $scope.$emit("environment:changed", $scope.environment);
      $scope.$hide();
    }
  })
angular.module('webappApp')
  .controller('UserOrgCtrl', function ($scope, User, Repo, Organization) {

    $scope.areaEditing = false;
    Organization.all("members").getList().then(function (data) {
      $scope.members = data.plain();
    })
    Organization.all("repos").getList().then(function (data) {
      $scope.repositories = data.plain();
    })
    Organization.all("scopes").getList().then(function (data) {
      $scope.areas = data.plain();
    })

    $scope.addArea = function () {
      $scope.areaEditing = true;
      $scope.area = {}
      $scope.area.members = [];
    }
    $scope.cancelArea = function () {
      $scope.areaEditing = false;
      $scope.area = {}
      $scope.area.members = [];
    }
    $scope.saveArea = function () {
      if (!$scope.area.number) {
        Organization.all("scopes").post($scope.area).then(function (data) {
          $scope.areas.push(data);
          $scope.cancelArea();
        })
      } else {
        var idx = $scope.areas.indexOf($scope.selectedArea);
        Organization.all("scopes").one($scope.area.number.toString()).patch($scope.area).then(function (data) {
          $scope.areas[idx] = data;
          $scope.cancelArea();
        })
      }
    }
    $scope.toggleBackup = function (member) {
      var idx = $scope.area.members.indexOf(member.name)
      if (idx == -1) {
        $scope.area.members.push(member.name);
      } else {
        $scope.area.members.splice(idx, 1);
      }
    }
    $scope.selectArea = function (area) {
      $scope.selectedArea = area;
      $scope.area = angular.copy(area);
      $scope.area.members = $scope.area.members.map(function (e) {
        return e.name;
      });
      if (area.owner) {
        $scope.area.owner = area.owner.name;
      }
      $scope.area.repository = area.repository.name;
      $scope.areaEditing = true;
    }
  });


angular.module('webappApp')
  .controller('ChangeSelectEnvironmentCtrl', function ($scope, User, Repo) {


    User.whoami().then(function (user) {
      $scope.user = user;
      User.environments().then(function (data) {
        $scope.environments = data.plain();
      })
    });

    $scope.save = function () {
      $scope.$emit("environment:changed", $scope.environment);
      $scope.$hide();
    }
  })
