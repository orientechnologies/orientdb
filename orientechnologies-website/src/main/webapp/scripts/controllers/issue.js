'use strict';
angular.module('webappApp')
  .controller('IssueCtrl', function ($scope, Organization, User, $routeParams) {

    $scope.githubIssue = GITHUB + "/" + ORGANIZATION;

    $scope.query = 'is:open '
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

    $scope.clear = function () {
      $scope.query = 'is:open '
      $scope.issue = {};
      $scope.search();
    }
    $scope.search = function (page) {
      if (!page) {
        $scope.page = 1;
      }
      Organization.all('issues').customGET("", {q: $scope.query, page: $scope.page}).then(function (data) {
        $scope.issues = data.content;
        $scope.pager = data.page;
        $scope.pager.pages = $scope.calculatePages($scope.pager);
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
      input = input.trim()
      return input += " " + name + ":\"" + val + "\" ";
    }
    var removeCondition = function (input, name, val) {
      return input.replace(" " + name + ":\"" + val + "\"", "");
    }
    $scope.$on("scope:changed", function (e, scope) {

      if ($scope.issue.scope) {
        $scope.query = removeCondition($scope.query, "area", $scope.issue.scope.name);
      }
      if (scope) {
        $scope.query = addCondition($scope.query, "area", scope.name)
        $scope.issue.scope = scope;
      }
      $scope.search();
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
    $scope.calculatePages = function (pager) {

      var maxBlocks, maxPage, maxPivotPages, minPage, numPages, pages;
      maxBlocks = 11;
      pages = [];
      var currentPage = pager.number;
      numPages = pager.totalPages;
      if (numPages > 1) {
        pages.push(1);
        maxPivotPages = Math.round((maxBlocks - 5) / 2);
        minPage = Math.max(2, currentPage - maxPivotPages);
        maxPage = Math.min(numPages - 1, currentPage + maxPivotPages * 2 - (currentPage - minPage));
        minPage = Math.max(2, minPage - (maxPivotPages * 2 - (maxPage - minPage)));
        var i = minPage;
        while (i <= maxPage) {
          if ((i === minPage && i !== 2) || (i === maxPage && i !== numPages - 1)) {
            pages.push(null);
          } else {
            pages.push(i);
          }
          i++;
        }
        pages.push(numPages);
        return pages
      }
    }
    $scope.getNumber = function (number) {
      return new Array(number);
    }
    $scope.changePage = function (val) {
      if (val > 0 && val <= $scope.pager.totalPages) {
        $scope.page = val;
        $scope.search(true);
      }
    }
    $scope.search();
  }
);

angular.module('webappApp')
  .controller('IssueNewCtrl', function ($scope, Organization, Repo, $location, User) {


    $scope.issue = {}
    $scope.save = function () {
      $scope.issue.scope = $scope.scope.number;
      Organization.all("issues").post($scope.issue).then(function (data) {
        $location.path("/issues/" + data.iid);
      });
    }
    Organization.all("scopes").getList().then(function (data) {
      $scope.scopes = data.plain();
    })
    User.whoami().then(function (data) {
      $scope.user = data;
      $scope.isMember = User.isMember(ORGANIZATION);
      $scope.isClient = User.isClient(ORGANIZATION);
      $scope.client = User.getClient(ORGANIZATION);
      User.environments().then(function (data) {
        $scope.environments = data;
      });
      if ($scope.client)
        $scope.issue.client = $scope.client.clientId;
      if ($scope.isMember) {
        Organization.all("clients").getList().then(function (data) {
          $scope.clients = data.plain();
        })
      }
    });


    Organization.all("priorities").getList().then(function (data) {
      $scope.priorities = data.plain();
    })
    $scope.$watch('issue.client', function (val) {
      if (val) {
        Organization.all("clients").one(val.toString()
        ).all('environments').getList().then(function (data) {
            $scope.environments = data.plain();
          })
      }
    });
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


    $scope.githubIssue = GITHUB + "/" + ORGANIZATION;

    $scope.number = $routeParams.id;
    var number = $scope.number;
    User.whoami().then(function (data) {
      $scope.isMember = User.isMember(ORGANIZATION);
      $scope.currentUser = data;
    });
    Organization.all("issues").one(number).get().then(function (data) {
      $scope.issue = data.plain();
      $scope.repo = $scope.issue.repository.name;
      User.whoami().then(function (data) {
        $scope.isOwner = $scope.issue.user.name == data.name;
      })
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

      if ($scope.newComment && $scope.newComment.body) {
        Repo.one($scope.repo).all("issues").one(number).all("comments").post($scope.newComment).then(function (data) {
          $scope.comments.push(data.plain());
          $scope.newComment.body = "";
        });
      }
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

    $scope.$on("environment:changed", function (e, environment) {

      Repo.one($scope.repo).all("issues").one(number).patch({environment: environment}).then(function (data) {
        $scope.issue.environment = environment;
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

    $scope.title = $scope.title || 'Apply labels to this issue';
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

    $scope.title = $scope.title || 'Change target milestone';
    $scope.isMilestoneSelected = function (milestone) {
      if ($scope.issue.milestone) {
        if (milestone.number) {
          return milestone.number == $scope.issue.milestone.number;
        } else {
          return milestone.title == $scope.issue.milestone.title;
        }
      } else {
        return false;
      }
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

    $scope.title = $scope.title || 'Change affected version';
    $scope.isVersionSelected = function (version) {
      if ($scope.issue.version) {
        if (version.number) {
          return version.number == $scope.issue.version.number;
        } else {
          return version.title == $scope.issue.version.title;
        }
      } else {
        return false;
      }

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
    $scope.title = $scope.title || 'Assign this issue';
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
    $scope.title = $scope.title || 'Change Area';
    $scope.isScoped = function (scope) {
      return $scope.issue.scope ? scope.name == $scope.issue.scope.name : false;
    }
    $scope.toggleScope = function (scope) {
      if (scope == null || !$scope.isScoped(scope)) {
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

angular.module('webappApp').controller('CommentController', function ($scope, Repo) {
  $scope.preview = true;


  $scope.clonedComment = {};
  $scope.cancelEditing = function () {
    $scope.preview = true;
    $scope.comment = $scope.clonedComment;
  }
  $scope.edit = function () {
    $scope.preview = false;
    $scope.clonedComment = angular.copy($scope.comment);
  }
  $scope.deleteComment = function () {

    Repo.one($scope.repo).all("issues").one($scope.number).all("comments").one($scope.comment.uuid).remove().then(function (data) {
      var idx = $scope.comments.indexOf($scope.comment);
      if (idx > -1) {
        $scope.comments.splice(idx, 1);
      }
    }).catch(function () {

    });
  }
  $scope.patchComment = function () {
    Repo.one($scope.repo).all("issues").one($scope.number).all("comments").one($scope.comment.uuid).patch($scope.comment).then(function (data) {
      $scope.preview = true;
    }).catch(function () {
      $scope.cancelEditing()
    });
  }
})
