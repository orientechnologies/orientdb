'use strict';
angular.module('webappApp')
  .controller('IssueCtrl', function ($scope, Organization, User, $routeParams) {

    $scope.query = 'is:open'
    $scope.page = 1;
    // Query By Example
    $scope.issue = {};
    if ($routeParams.q) {
      $scope.query = $routeParams.q;
    }
    if ($routeParams.page) {
      $scope.page = $routeParams.page;
    }

    $scope.labelPopover = {
      close: true
    }

    $scope.search = function () {
      Organization.all('issues').customGET("", {q: $scope.query, page: $scope.page}).then(function (data) {
        $scope.issues = data.content;
        $scope.pager = data.page;
      });
    }

    User.whoami().then(function (data) {
      $scope.isMember = User.isMember(ORGANIZATION);

      if ($scope.isMember) {
        Organization.all("clients").getList().then(function (data) {
          $scope.clients = data.plain();
        })
      }
    });
    Organization.all("scopes").getList().then(function (data) {
      $scope.scopes = data.plain();
    })
    Organization.all("members").getList().then(function (data) {
      $scope.assignees = data.plain();
    })
    Organization.all("priorities").getList().then(function (data) {
      $scope.priorities = data.plain();
    })
    Organization.all("milestones").getList().then(function (data) {
      $scope.milestones = data.plain();
      $scope.versions = data.plain();
    })
    Organization.all("labels").getList().then(function (data) {
      $scope.labels = data.plain();
    })

    var addCondition = function (input, name, val) {
      return input += " " + name + ":\"" + val + "\"";
    }
    var removeCondition = function (input, name, val) {
      return input.replace(" " + name + ":\"" + val + "\"", "");
    }
    $scope.$on("scope:changed", function (e, scope) {
      if (scope) {
        if ($scope.issue.scope) {
          $scope.query = removeCondition($scope.query, "area", $scope.issue.scope.name);
        }
        $scope.query = addCondition($scope.query, "area", scope.name)
        $scope.issue.scope = scope;
        $scope.search();
      }
    });
    $scope.$on("client:changed", function (e, client) {
      if (client) {
        if ($scope.issue.client) {
          $scope.query = removeCondition($scope.query, "client", $scope.issue.client.name);
        }
        $scope.query = addCondition($scope.query, "client", client.name)
        $scope.issue.client = client;
        $scope.search();
      }
    });
    $scope.$on("priority:changed", function (e, priority) {
      if (priority) {
        if ($scope.issue.priority) {
          $scope.query = removeCondition($scope.query, "priority", $scope.issue.priority.name);
        }
        $scope.query = addCondition($scope.query, "priority", priority.name);
        $scope.issue.priority = priority;
        $scope.search();
      }
    });
    $scope.$on("assignee:changed", function (e, assignee) {
      if (assignee) {
        if ($scope.issue.assignee) {
          $scope.query = removeCondition($scope.query, "assignee", $scope.issue.assignee.name);
        }
        $scope.query = addCondition($scope.query, "assignee", assignee.name);
        $scope.issue.assignee = assignee;
        $scope.search();
      }
    });
    $scope.$on("milestone:changed", function (e, milestone) {
      if (milestone) {
        if ($scope.issue.milestone) {
          $scope.query = removeCondition($scope.query, "milestone", $scope.issue.milestone.title);
        }
        $scope.query = addCondition($scope.query, "milestone", milestone.title);
        $scope.issue.milestone = milestone;
        $scope.search();
      }
    });
    $scope.$on("version:changed", function (e, version) {
      if (version) {
        if ($scope.issue.version) {
          $scope.query = removeCondition($scope.query, "version", $scope.issue.version.title);
        }
        $scope.query = addCondition($scope.query, "version", version.title);
        $scope.issue.version = version;
        $scope.search();
      }
    });
    $scope.$on("label:added", function (e, label) {
      if (label) {
        $scope.query = addCondition($scope.query, "label", label.name);
        if (!$scope.issue.labels) {
          $scope.issue.labels = [];
        }
        $scope.issue.labels.push(label)
        $scope.search();
      }
    });
    $scope.$on("label:removed", function (e, label) {
      if (label) {
        $scope.query = removeCondition($scope.query, "label", label.name);
        var idx = $scope.issue.labels.indexOf(label);
        $scope.issue.labels.splice(idx, 1);
        $scope.search();
      }
    });
    $scope.getNumber = function (number) {
      return new Array(number);
    }
    $scope.changePage = function (val) {
      if (val > 0 && val <= $scope.pager.totalPages) {
        $scope.page = val;
        $scope.search();
      }
    }
    $scope.search();
  });

angular.module('webappApp')
  .controller('IssueNewCtrl', function ($scope, Organization, Repo, $location) {


    $scope.save = function () {
      $scope.issue.scope = $scope.scope.number;
      Organization.all("issues").post($scope.issue).then(function (data) {
        $location.path("/issues/" + data.iid);
      });
      //Repo.one($scope.repo.name).all("issues").post($scope.issue).then(function (data) {
      //  $location.path("/issues/" + $scope.repo.name + "@" + data.uuid);
      //});
    }
    Organization.all("scopes").getList().then(function (data) {
      $scope.scopes = data.plain();
    })
    Organization.all("priorities").getList().then(function (data) {
      $scope.priorities = data.plain();
    })
    $scope.$watch("scope", function (val) {
      if (val && val.repository) {
        Repo.one(val.repository.name).all("teams").getList().then(function (data) {
          $scope.assignees = data.plain();
        })
        Repo.one(val.repository.name).all("milestones").getList().then(function (data) {
          $scope.milestones = data.plain();
        });
      }
    });
  });
angular.module('webappApp')
  .controller('IssueEditCtrl', function ($scope, $routeParams, Organization, Repo, $popover, $route, User) {

    //var id = $routeParams.id.split("@");
    //
    //var repo = id[0];
    //var number = id[1];
    var number = $routeParams.id;

    User.whoami().then(function (data) {
      $scope.isMember = User.isMember(ORGANIZATION);
    });
    Organization.all("issues").one(number).get().then(function (data) {
      $scope.issue = data.plain();
      $scope.repo = $scope.issue.repository.name;
      refreshEvents();
      initTypologic();
    });
    //Repo.one(repo).all("issues").one(number).get().then(function (data) {
    //  $scope.issue = data.plain();
    //});
    Organization.all("priorities").getList().then(function (data) {
      $scope.priorities = data.plain();
    })
    $scope.getEventTpl = function (e) {
      return 'views/issues/events/' + e + ".html";
    }
    function refreshEvents() {
      $scope.comments = Repo.one($scope.repo).all("issues").one(number).all("events").getList().$object;
    }


    function initTypologic() {
      $scope.labels = Repo.one($scope.repo).all("labels").getList().$object;
      Repo.one($scope.repo).all("milestones").getList().then(function (data) {
        $scope.versions = data.plain();
        $scope.milestones = data.plain();
      });
      Repo.one($scope.repo).all("scopes").getList().then(function (data) {
        $scope.scopes = data.plain();
      });
      Repo.one($scope.repo).all("teams").getList().then(function (data) {
        $scope.assignees = data.plain();
      })
    }

    initTypologic();
    $scope.sync = function () {
      Repo.one($scope.repo).all("issues").one(number).all("sync").post().then(function (data) {
        $route.reload();
      });
    }
    $scope.comment = function () {

      Repo.one($scope.repo).all("issues").one(number).all("comments").post($scope.newComment).then(function (data) {
        $scope.comments.push(data.plain());
        $scope.newComment.body = "";
      });
    }

    $scope.close = function () {
      Repo.one($scope.repo).all("issues").one(number).patch({state: "closed"}).then(function (data) {
        $scope.issue.state = "closed";
        refreshEvents();
      });
    }
    $scope.reopen = function () {
      Repo.one($scope.repo).all("issues").one(number).patch({state: "open"}).then(function (data) {
        $scope.issue.state = "open";
        refreshEvents();
      });
    }
    // CHANGE LABEL EVENT
    $scope.$on("label:added", function (e, label) {

      Repo.one($scope.repo).all("issues").one(number).all("labels").post([label.name]).then(function (data) {
        $scope.issue.labels.push(label);
        refreshEvents();
      });


    })
    $scope.$on("label:removed", function (e, label) {


      Repo.one($scope.repo).all("issues").one(number).one("labels", label.name).remove().then(function () {
        var idx = $scope.issue.labels.indexOf(label);
        $scope.issue.labels.splice(idx, 1);
        refreshEvents();
      });
    })

    // CHANGE VERSION EVENT

    $scope.$on("version:changed", function (e, version) {

      Repo.one($scope.repo).all("issues").one(number).patch({version: version.number}).then(function (data) {
        $scope.issue.version = version;
        refreshEvents();
      })


    });
    // CHANGE MILESTONE EVENT
    $scope.$on("milestone:changed", function (e, milestone) {

      Repo.one($scope.repo).all("issues").one(number).patch({milestone: milestone.number}).then(function (data) {
        $scope.issue.milestone = milestone;
        refreshEvents();
      })


    });

    $scope.$on("assignee:changed", function (e, assignee) {
      Repo.one($scope.repo).all("issues").one(number).patch({assignee: assignee.name}).then(function (data) {
        $scope.issue.assignee = assignee;
        refreshEvents();
      })


    });
    $scope.$on("priority:changed", function (e, priority) {
      Repo.one($scope.repo).all("issues").one(number).patch({priority: priority.number}).then(function (data) {
        $scope.issue.priority = priority;
        refreshEvents();
      })
    });
    $scope.$on("scope:changed", function (e, scope) {
      Repo.one($scope.repo).all("issues").one(number).patch({scope: scope.number}).then(function (data) {
        $scope.issue.scope = scope;
        refreshEvents();
      })
    });
  });

angular.module('webappApp')
  .controller('ChangeLabelCtrl', function ($scope) {

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
      if ($scope.labelPopover && $scope.labelPopover) {
        $scope.$hide();
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
  .controller('ChangeVersionCtrl', function ($scope) {

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
  .controller('ChangeAssigneeCtrl', function ($scope) {
    $scope.isAssigneeSelected = function (assignee) {

      return ($scope.issue.assignee && assignee) ? assignee.name == $scope.issue.assignee.name : false;
    }
    $scope.toggleAssignee = function (assignee) {
      if (!$scope.isAssigneeSelected(assignee)) {
        $scope.$emit("assignee:changed", assignee);
        $scope.$hide();
      }
    }

  });

angular.module('webappApp')
  .controller('ChangePriorityCtrl', function ($scope) {

    $scope.isPrioritized = function (priority) {
      return $scope.issue.priority ? priority.name == $scope.issue.priority.name : false;
    }
    $scope.togglePriority = function (priority) {
      if (!$scope.isPrioritized(priority)) {
        $scope.$emit("priority:changed", priority);
        $scope.$hide();
      }
    }

  });

angular.module('webappApp')
  .controller('ChangeScopeCtrl', function ($scope) {

    $scope.isScoped = function (scope) {
      return $scope.issue.scope ? scope.name == $scope.issue.scope.name : false;
    }
    $scope.toggleScope = function (scope) {
      if (!$scope.isScoped(scope)) {
        $scope.$emit("scope:changed", scope);
      }
      $scope.$hide();
    }

  });
angular.module('webappApp')
  .controller('ChangeClientCtrl', function ($scope) {

    $scope.isClientSelected = function (client) {
      return $scope.issue.client ? client.name == $scope.issue.client.name : false;
    }
    $scope.toggleClient = function (client) {
      if (!$scope.isClientSelected(client)) {
        $scope.$emit("client:changed", client);
      }
      $scope.$hide();
    }

  });


