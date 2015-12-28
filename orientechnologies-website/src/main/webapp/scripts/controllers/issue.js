'use strict';
angular.module('webappApp')
  .controller('IssueCtrl', function ($scope, Organization, User, $routeParams, $location) {

      $scope.githubIssue = GITHUB + "/" + ORGANIZATION;


      $scope.sorts = [{
        name: 'Newest',
        filter: 'createdAt-desc'
      }, {
        name: 'Oldest',
        filter: 'createdAt-asc'
      }, {
        name: 'More Priority',
        filter: 'priority-desc'
      }, {
        name: 'Less Priority',
        filter: 'priority-asc'

      }, {
        name: 'Due Date',
        filter: 'dueTime-asc'
      }]

      $scope.query = 'is:open '
      $scope.page = 1;
      // Query By Example
      $scope.issue = {};
      $scope.matched = {};
      if ($routeParams.q) {
        $scope.query = $routeParams.q;
        var match = $scope.query.match(/(?:[^\s"]+|"[^"]*")+/g);

        match.forEach(function (m) {
          var splitted = m.split(":")
          if (splitted[0] == "label") {
            if (!$scope.matched[splitted[0]]) {
              $scope.matched[splitted[0]] = []
            }
            $scope.matched[splitted[0]].push(splitted[1].replace(/"/g, ""));
          } else {
            if (splitted[1]) {
              $scope.matched[splitted[0]] = splitted[1].replace(/"/g, "");
            } else {
              $scope.matched['fulltext'] = splitted[0];
            }
          }
        })
        $scope.sorts.forEach(function (s) {
          if (s.filter == $scope.matched['sort']) {
            $scope.issue.sort = s;
          }
        });

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
      $scope.changeRoute = function () {

      }
      $scope.searchForIssue = function () {

        Organization.all('issues').customGET("", {q: $scope.query, page: $scope.page}).then(function (data) {
          $scope.issues = data.content;
          $scope.pager = data.page;
          $scope.pager.pages = $scope.calculatePages($scope.pager);
        });
      }
      $scope.search = function (page) {
        if (!page) {
          $scope.page = 1;
        }
        $location.search({'q': $scope.query, 'page': $scope.page});
      }

      User.whoami().then(function (data) {
        $scope.isMember = User.isMember(ORGANIZATION);
        $scope.isSupport = User.isSupport(ORGANIZATION);
        if ($scope.isMember || $scope.isSupport) {
          Organization.all("clients").getList().then(function (data) {
            $scope.clients = data.plain();
            $scope.clients.forEach(function (a) {
              if (a.name == $scope.matched['client']) {
                $scope.issue.client = a;
              }
            })
            if (!$scope.issue.client && $scope.matched['client'] == "_my") {
              $scope.issue.client = {name: "_my"};
            }
            if (!$scope.issue.client && $scope.matched['client'] == "_all") {
              $scope.issue.client = {name: "_all"};
            }
          })
        }
      });
      Organization.all("scopes").getList().then(function (data) {
        $scope.scopes = data.plain();


        $scope.scopes.forEach(function (s) {
          if (s.name == $scope.matched['area']) {
            $scope.issue.scope = s;
          }
        })


      })
      Organization.all("members").getList().then(function (data) {
        $scope.assignees = data.plain();

        $scope.assignees.forEach(function (a) {
          if (a.name == $scope.matched['assignee']) {
            $scope.issue.assignee = a;
          }
        })
      })
      Organization.all("priorities").getList().then(function (data) {
        $scope.priorities = data.plain();
        $scope.priorities.forEach(function (p) {
          if (p.name == $scope.matched['priority']) {
            $scope.issue.priority = p;
          }
        })
      })
      Organization.all("milestones").getList().then(function (data) {
        $scope.milestones = data.plain();
        $scope.versions = data.plain();

        $scope.milestones.forEach(function (m) {
          if (m.title == $scope.matched['milestone']) {
            $scope.issue.milestone = m;
          }
          if (m.title == $scope.matched['version']) {
            $scope.issue.version = m;
          }
        })
      })
      Organization.all("labels").getList().then(function (data) {
        $scope.labels = data.plain();
        $scope.issue.labels = [];
        $scope.labels.forEach(function (l) {
          if ($scope.matched["label"] && $scope.matched["label"].indexOf(l.name) != -1) {
            $scope.issue.labels.push(l);
          }
        })

      })
      Organization.all('repos').getList().then(function (data) {
        $scope.repositories = data.plain();

        $scope.repositories.forEach(function (m) {
          if (m.name == $scope.matched['repo']) {
            $scope.issue.repository = m;
          }
        })
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
        if ($scope.issue.client) {
          $scope.query = removeCondition($scope.query, "client", $scope.issue.client.name);
        }
        if (client) {
          $scope.query = addCondition($scope.query, "client", client.name)
        }

        $scope.issue.client = client;
        $scope.search();
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

        if ($scope.issue.assignee) {
          $scope.query = removeCondition($scope.query, "assignee", $scope.issue.assignee.name);
        }
        if (assignee) {
          $scope.query = addCondition($scope.query, "assignee", assignee.name);

          $scope.issue.assignee = assignee;
          $scope.search();
        }
      });
      $scope.$on("repo:changed", function (e, repo) {
        if (repo) {
          if ($scope.issue.repo) {
            $scope.query = removeCondition($scope.query, "repo", $scope.issue.repo.name);
          }
          $scope.query = addCondition($scope.query, "repo", repo.name);
          $scope.issue.repo = repo;
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
      $scope.$on("sort:changed", function (e, sort) {
        if (sort) {
          if ($scope.issue.sort) {
            $scope.query = removeCondition($scope.query, "sort", $scope.issue.sort.filter);
          }
          $scope.issue.sort = sort;

          $scope.query = addCondition($scope.query, "sort", sort.filter);
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
      $scope.searchForIssue();
    }
  );

angular.module('webappApp')
  .controller('IssueNewCtrl', function ($scope, Organization, Repo, $location, User) {

    $scope.carriage = true;

    $scope.placeholder = "Leave a comment (Supports Markdown)";
    $scope.types = {
      'Bug': 'bug',
      'Performance': 'performance',
      'Documentation': 'documentation',
      'Enhancement': 'enhancement',
      'Question': 'question'
    }
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
    $scope.labels = $scope.types;
    User.whoami().then(function (data) {
      $scope.user = data;
      $scope.isMember = User.isMember(ORGANIZATION);
      $scope.isClient = User.isClient(ORGANIZATION);
      $scope.isSupport = User.isSupport(ORGANIZATION);
      $scope.client = User.getClient(ORGANIZATION);

      if ($scope.isClient && !$scope.isSupport) {
        $scope.issue.confidential = true;
      }
      User.environments().then(function (data) {
        $scope.environments = data.plain();
      });
      if ($scope.client)
        $scope.issue.client = $scope.client.clientId;
      if ($scope.isMember || $scope.isSupport) {
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
  .controller('IssueEditCtrl', function ($scope, $routeParams, Organization, Repo, $popover, $route, User, $timeout, $location, $q, Notification) {


    $scope.max = MAX_ATTACHMENT;
    $scope.carriage = true;
    $scope.githubIssue = GITHUB + "/" + ORGANIZATION;

    $scope.placeholder = "Leave a Comment (Supports Markdown)";
    var waiting_reply = 'waiting reply';
    var in_progress = 'in progress';
    var question = 'question';
    $scope.number = $routeParams.id;
    var number = $scope.number;
    User.whoami().then(function (data) {


      $scope.isMember = User.isMember(ORGANIZATION);
      $scope.isSupport = User.isSupport(ORGANIZATION);
      $scope.isClient = User.isClient(ORGANIZATION);
      $scope.isContributor = User.isContributor(ORGANIZATION);
      $scope.currentUser = data;

      if ($scope.isMember || $scope.isSupport) {
        Organization.all("clients").getList().then(function (data) {
          $scope.clients = data.plain();
        })
      }
    });


    $scope.canAssignIssue = function () {
      if ($scope.issue) {
        return $scope.isMember || $scope.isSupport || ($scope.isContributor && $scope.issue.confidential);
      }
      return false;
    }
    Organization.all("issues").one(number).get().then(function (data) {
      $scope.issue = data.plain();


      $scope.repo = $scope.issue.repository.name;
      if ($scope.issue.confidential) {
        $scope.url = Repo.one($scope.repo).all("issues").one(number).all('attachments').getRequestedUrl();
        Repo.one($scope.repo).all("issues").one(number).all('attachments').getList().then(function (data) {
          $scope.attachments = data.plain();
        });
      }
      $scope.githubCommit = GITHUB + "/" + ORGANIZATION + "/" + $scope.repo + "/commit/";
      User.whoami().then(function (data) {
        $scope.isOwner = $scope.issue.user.name == data.name;
        if ($scope.issue.client) {
          try {
            $scope.isOwnerClient = $scope.isClient && ($scope.issue.client.clientId == data.clients[0].clientId);
          } catch (e) {
            $scope.isOwnerClient = false;
          }
        }
      })
      Repo.one($scope.repo).all("issues").one(number).all("actors").getList().then(function (data) {
        $scope.actors = data.plain();
      });

      $scope.isInWait = function () {
        var found = false;
        $scope.issue.labels.forEach(function (l) {
          found = found || l.name == waiting_reply;
        })
        return found;
      };
      $scope.issue.labels
      refreshEvents();
      initTypologic();
    }).catch(function (e) {
      $location.path("/issues");
    });
    Organization.all("priorities").getList().then(function (data) {
      $scope.priorities = data.plain();
    })
    $scope.getEventTpl = function (e) {
      return 'views/issues/events/' + e + ".html";
    }
    function refreshEvents() {
      Repo.one($scope.repo).all("issues").one(number).all("events").getList().then(function (data) {


        $scope.comments = data.plain().filter(function (e) {
          return (e.event != 'mentioned' && e.event != 'subscribed')
        })
      });
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

      var promises = []

      promises.push(Repo.one($scope.repo).all("teams").getList());
      promises.push(Organization.all('contributors').getList());

      $q.all(promises).then(function (results) {
        if ($scope.issue.confidential) {
          $scope.assignees = results[0].plain().concat(results[1].plain());
        } else {
          $scope.assignees = results[0].plain();
        }
      });

    }


    $scope.removeAttachment = function (attachment) {

      Repo.one($scope.repo).all("issues").one(number).all('attachments').one(encodeURI(attachment.name)).remove().then(function (response) {
        var idx = $scope.attachments.indexOf(attachment);
        if (idx != -1) {
          $scope.attachments.splice(idx, 1);
          Notification.success("File " + attachment.name + " removed correctly from issue " + $scope.issue.iid + ".")
        }
      }).catch(function (error) {
        var msg = "Error removing file " + attachment.name + ". " + error.statusText + " (" + error.status + ")";
        Notification.error(msg);
      })
    }
    $scope.downloadAttachment = function (attachment) {

      Repo.one($scope.repo).all("issues").one(number).all('attachments').one(encodeURI(attachment.name)).withHttpConfig({responseType: 'blob'}).get().then(function (response) {

        saveAs(response, attachment.name);

      }).catch(function (error) {
        var msg = "Error downloading file " + attachment.name + ". " + error.statusText + " (" + error.status + ")";
        Notification.error(msg);
      })
    }

    $scope.sync = function () {
      Repo.one($scope.repo).all("issues").one(number).all("sync").post().then(function (data) {
        $route.reload();
      });
    }
    $scope.escalateIssue = function () {
      Repo.one($scope.repo).all("issues").one(number).all("escalate").post().then(function (data) {
        var jacked = humane.create({baseCls: 'humane-jackedup', addnCls: 'humane-jackedup-success'})
        jacked.log("Escalation email has been sent.");
      }).catch(function (e) {

      });
    }
    $scope.comment = function () {


      var deferred = $q.defer();
      if ($scope.newComment && $scope.newComment.body) {
        Repo.one($scope.repo).all("issues").one(number).all("comments").post($scope.newComment).then(function (data) {
          if (data) {
            $scope.comments.push(data.plain());
            $scope.newComment.body = "";
            refreshEvents();
          } else {
            $scope.newComment.body = "";
            $timeout(function () {
              refreshEvents();
            }, 2000)
          }
          deferred.resolve();
        });
      }
      return deferred.promise;
    }


    $scope.markAsQuestion = function () {
      $scope.newComment = {}
      $scope.newComment.body = "This is more a question than an issue. Please post it on StackOverflow http://stackoverflow.com/questions/tagged/orientdb"
      $scope.comment().then(function (data) {

        $scope.close().then(function () {
          $scope.addLabel(question);
        })

      });
    }
    $scope.commentAndWait = function () {

      $scope.comment().then(function (data) {

        $scope.addLabel(waiting_reply);
      })
    }
    $scope.$watch("newComment.body", function (data) {
      if (data && data != "") {
        $scope.closeComment = true;
      } else {
        $scope.closeComment = false;
      }
    })
    $scope.changeTitle = function (title) {
      Repo.one($scope.repo).all("issues").one(number).patch({title: title}).then(function (data) {
        $scope.issue.title = title;
        $scope.newTitle = null;
        $scope.editingTitle = false;
        refreshEvents();
      });
    }
    $scope.copyIssueNumber = function () {

      var copyEvent = new ClipboardEvent('copy', {dataType: 'text/plain', data: 'Data to be copied'});

      document.dispatchEvent(copyEvent);
    }

    $scope.$on('file-uploaded', function (evt, file) {

      $scope.attachments.push(file);
      $scope.$apply();
      Notification.success("File " + file.name + " attached correctly to issue " + $scope.issue.iid + ".")
    })
    $scope.$on('file-uploaded-error', function (evt, err, code) {
      Notification.error(err);
    })
    $scope.close = function () {


      var deferred = $q.defer();
      Repo.one($scope.repo).all("issues").one(number).patch({state: "closed"}).then(function (data) {
        $scope.issue.state = "CLOSED";
        if ($scope.newComment && $scope.newComment.body) {
          $scope.comment();
        }
        refreshEvents();
        $scope.removeLabel(in_progress);
        deferred.resolve();
      });
      return deferred.promise;


    }
    $scope.removeLabel = function (l) {

      var localLabel = null;
      $scope.issue.labels.forEach(function (label) {
        if (label.name == l) {
          localLabel = label;
        }
      });

      if (localLabel) {
        $scope.$emit("label:removed", localLabel);
      }
    }
    $scope.addLabel = function (l) {
      var localLabel = null;
      $scope.labels.forEach(function (label) {
        if (label.name == l) {
          localLabel = label;
        }
      });

      $scope.issue.labels.forEach(function (label) {
        if (label.name == l) {
          localLabel = null;
        }
      });

      if (localLabel) {
        $scope.$emit("label:added", localLabel);
      }
    }
    $scope.reopen = function () {
      Repo.one($scope.repo).all("issues").one(number).patch({state: "open"}).then(function (data) {
        $scope.issue.state = "OPEN";
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
    $scope.$on("client:changed", function (e, client) {

      Repo.one($scope.repo).all("issues").one(number).patch({client: client.clientId}).then(function (data) {
        $scope.issue.client = client;
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

    //$scope.pollIssue = function () {
    //  $timeout(function () {
    //    Organization.all("issues").one(number).get().then(function (data) {
    //      $scope.issuePolled = data.plain();
    //
    //      if($)
    //    })
    //  }, 2000);
    //}
  });

angular.module('webappApp')
  .controller('ChangeLabelCtrl', function ($scope, $filter) {

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

    $scope.selectFirst = function () {

      var filtered = $filter('filter')($scope.labels, $scope.labelFilter);
      if (filtered.length == 1) {
        $scope.toggleLabel(filtered[0]);
      }
    }
  });

angular.module('webappApp')
  .controller('ChangeTagCtrl', function ($scope, $filter) {

    $scope.title = $scope.title || 'Apply tags to this topic';
    $scope.isTagged = function (tag) {

      for (var l in $scope.topic.tags) {
        if ($scope.topic.tags[l].name == tag.name) {
          return true;
        }
      }
      return false;
    }
    $scope.toggleTag = function (tag) {
      if (!$scope.isTagged(tag)) {
        $scope.$emit("tag:added", tag);
      } else {
        $scope.$emit("tag:removed", tag);
      }
      if ($scope.labelPopover && $scope.labelPopover) {
        $scope.$hide();
      }
    }

    $scope.selectFirst = function () {

      var filtered = $filter('filter')($scope.tags, $scope.labelFilter);
      if (filtered.length == 1) {
        $scope.toggleTag(filtered[0]);
      }
    }
  });

angular.module('webappApp')
  .controller('ChangeMilestoneCtrl', function ($scope, $filter, $routeParams, Repo, $popover) {

    $scope.title = $scope.title || 'Change target milestone';
    $scope.isMilestoneSelected = function (milestone) {
      if (!milestone) return false;
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
      if (!milestone) {
        $scope.$emit("milestone:changed", milestone);
        $scope.$hide();
      } else if (!$scope.isMilestoneSelected(milestone)) {
        $scope.$emit("milestone:changed", milestone);
        $scope.$hide();
      }
    }

    $scope.selectFirst = function () {

      var filtered = $filter('filter')($scope.milestones, $scope.filter);
      if (filtered.length == 1) {
        $scope.toggleMilestone(filtered[0]);
      }
    }
  });
angular.module('webappApp')
  .controller('ChangeVersionCtrl', function ($scope, $filter) {

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

    $scope.selectFirst = function () {

      var filtered = $filter('filter')($scope.versions, $scope.filter);
      if (filtered.length == 1) {
        $scope.toggleVersion(filtered[0]);
      }
    }
  });
angular.module('webappApp')
  .controller('ChangeAssigneeCtrl', function ($scope, $filter) {
    $scope.title = $scope.title || 'Assign this issue';

    $scope.isAssigneeSelected = function (assignee) {
      return ($scope.issue.assignee && assignee) ? assignee.name == $scope.issue.assignee.name : false;
    }
    $scope.toggleAssignee = function (assignee) {
      if (!assignee) {
        $scope.$emit("assignee:changed", assignee);
        $scope.$hide();
      } else if (!$scope.isAssigneeSelected(assignee)) {
        $scope.$emit("assignee:changed", assignee);
        $scope.$hide();
      }
    }

    $scope.selectFirst = function () {

      var filtered = $filter('filter')($scope.assignees, $scope.filter);
      if (filtered.length == 1) {
        $scope.toggleAssignee(filtered[0]);
      }
    }
  });

angular.module('webappApp')
  .controller('ChangePriorityCtrl', function ($scope, $filter) {

    $scope.isPrioritized = function (priority) {
      return $scope.issue.priority ? priority.name == $scope.issue.priority.name : false;
    }
    $scope.togglePriority = function (priority) {
      if (!$scope.isPrioritized(priority)) {
        $scope.$emit("priority:changed", priority);
        $scope.$hide();
      }
    }

    $scope.selectFirst = function () {

      var filtered = $filter('filter')($scope.priorities, $scope.filter);
      if (filtered.length == 1) {
        $scope.togglePriority(filtered[0]);
      }
    }
  });

angular.module('webappApp')
  .controller('ChangeScopeCtrl', function ($scope, $filter) {
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

    $scope.selectFirst = function () {

      var filtered = $filter('filter')($scope.scopes, $scope.filter);
      if (filtered.length == 1) {
        $scope.toggleScope(filtered[0]);
      }
    }
  });
angular.module('webappApp')
  .controller('ChangeRepoCtrl', function ($scope, $filter) {
    $scope.title = $scope.title || 'Change Repository';
    $scope.isRepo = function (repository) {
      return $scope.issue.repository ? repository.name == $scope.issue.repository.name : false;
    }
    $scope.toggleRepo = function (repo) {
      if (repo == null || !$scope.isRepo(repo)) {
        $scope.$emit("repo:changed", repo);
      }
      $scope.$hide();
    }

    $scope.selectFirst = function () {

      var filtered = $filter('filter')($scope.repositories, $scope.filter);
      if (filtered.length == 1) {
        $scope.toggleRepo(filtered[0]);
      }
    }

  });
angular.module('webappApp')
  .controller('ChangeClientCtrl', function ($scope, $filter) {


    $scope.allClient = {name: "_all"}
    $scope.mockClient = {name: "_my"}
    $scope.isClientSelected = function (client) {
      return $scope.issue.client ? client.name == $scope.issue.client.name : false;
    }
    $scope.toggleClient = function (client) {
      $scope.$emit("client:changed", client);
      $scope.$hide();
    }

    $scope.selectFirst = function () {

      var filtered = $filter('filter')($scope.clients, $scope.queryClient);
      if (filtered.length == 1) {
        $scope.toggleClient(filtered[0]);
      }
    }
  });

angular.module('webappApp')
  .controller('ChangeSortCtrl', function ($scope) {


    $scope.isSortSelected = function (sort) {
      return $scope.issue.sort ? sort.name == $scope.issue.sort.name : false;
    }
    $scope.toggleSort = function (sort) {
      if (!$scope.isSortSelected(sort)) {
        $scope.$emit("sort:changed", sort);
      }
      $scope.$hide();
    }

  });
angular.module('webappApp').controller('CommentController', function ($scope, Repo) {
  $scope.preview = true;
  $scope.carriage = true;


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

angular.module('webappApp').controller('BodyController', function ($scope, Repo) {
  $scope.preview = true;
  $scope.carriage = true;

  $scope.clonedComment = {};
  $scope.cancelEditing = function () {
    $scope.preview = true;
    $scope.issue.body = $scope.clonedComment;
  }
  $scope.edit = function () {
    $scope.preview = false;
    $scope.clonedComment = angular.copy($scope.issue.body);
  }
  $scope.patchComment = function () {
    Repo.one($scope.repo).all("issues").one($scope.number).patch({body: $scope.issue.body}).then(function (data) {
      $scope.preview = true;
    }).catch(function () {
      $scope.cancelEditing()
    });
  }
})
