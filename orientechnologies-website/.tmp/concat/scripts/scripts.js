'use strict';

/**
 * @ngdoc overview
 * @name webappApp
 * @description
 * # webappApp
 *
 * Main module of the application.
 */
angular
  .module('webappApp', [
    'ngAnimate',
    'ngCookies',
    'ngResource',
    'ngRoute',
    'ngSanitize',
    'ngTouch',
    'restangular',
    'ngMoment',
    'ngCookies',
    'mgcrea.ngStrap',
    'ngUtilFilters',
    'ngStorage',
    'mentio',
    'luegg.directives',
    'scroll',
    'utils.autofocus',
    'angular-otobox',
    'ngTagsInput'
  ])
  .config(["$routeProvider", "$httpProvider", "RestangularProvider", function ($routeProvider, $httpProvider, RestangularProvider) {
    $routeProvider
      .when('/', {
        templateUrl: 'views/main.html',
        controller: 'MainCtrl'
      })
      .when('/about', {
        templateUrl: 'views/about.html',
        controller: 'AboutCtrl'
      })
      .when('/login', {
        templateUrl: 'views/login.html',
        controller: 'LoginCtrl'
      })
      .when('/users/:username', {
        templateUrl: 'views/user.html',
        controller: 'UserCtrl'
      })
      .when('/issues', {
        templateUrl: 'views/issues.html',
        controller: 'IssueCtrl'
      })
      .when('/issues/new', {
        templateUrl: 'views/issues/newIssue.html',
        controller: 'IssueNewCtrl'
      })
      .when('/issues/:id', {
        templateUrl: 'views/issues/editIssue.html',
        controller: 'IssueEditCtrl'
      })
      .when('/clients', {
        templateUrl: 'views/clients.html',
        controller: 'ClientCtrl'
      })
      .when('/rooms', {
        templateUrl: 'views/room.html',
        controller: 'ChatCtrl'
      })
      .when('/rooms/:id', {
        templateUrl: 'views/room.html',
        controller: 'ChatCtrl'
      })
      .when('/topics', {
        templateUrl: 'views/topics.html',
        controller: 'TopicCtrl'
      })
      .when('/topics/new', {
        templateUrl: 'views/topics/topicNew.html',
        controller: 'TopicNewCtrl'
      })
      .when('/topics/:id', {
        templateUrl: 'views/topics/topicEdit.html',
        controller: 'TopicEditCtrl'
      })
      .when('/clients/new', {
        templateUrl: 'views/clients/newClient.html',
        controller: 'ClientNewCtrl'
      })
      .when('/clients/:id', {
        templateUrl: 'views/clients/editClient.html',
        controller: 'ClientEditCtrl'
      })
      .otherwise({
        redirectTo: '/'
      });

    $httpProvider.interceptors.push('oauthHttpInterceptor');
    RestangularProvider.setBaseUrl('/api/v1');

    $httpProvider.interceptors.push(["$q", "$location", function ($q, $location) {
      return {
        responseError: function (rejection) {

          if (rejection.status == 401 || rejection.status == 403) {
            $location.path("/login")
          }
          return $q.reject(rejection);
        }
      };
    }]);
  }]).run(["$rootScope", "ChatService", function ($rootScope, ChatService) {
    $rootScope.$on("$routeChangeSuccess",
      function (event, current, previous, rejection) {

        if (ChatService.connected) {
          ChatService.disconnect();
        }
        if (current.loadedTemplateUrl == 'views/room.html') {
          ChatService.connect();
        }


      });
  }]);
angular.module('webappApp').factory('oauthHttpInterceptor', ["$cookies", "AccessToken", function ($cookies, AccessToken) {
  return {
    request: function (config) {
      if ($cookies.prjhub_token) {
        AccessToken.set($cookies.prjhub_token);
        delete $cookies.prjhub_token;
      }
      var token = AccessToken.get();

      if (token) {
        config.headers['X-AUTH-TOKEN'] = token;
      }
      return config;
    }
  };
}]);


var API = "v1/"
var ORGANIZATION = 'orientechnologies';
//var ORGANIZATION = 'romeshell';
//var DEFAULT_REPO = 'shell-notifications';
var DEFAULT_REPO = 'orientdb';
var GITHUB = "https://github.com"

if (location.hostname == 'localhost') {
  var WEBSOCKET = "ws://" + location.host + "/chat"
}
else {
  var WEBSOCKET = "ws://" + location.hostname + "/chat";
}

String.prototype.capitalize = function () {
  return this.charAt(0).toUpperCase() + this.slice(1);
}

'use strict';

/**
 * @ngdoc function
 * @name webappApp.controller:MainCtrl
 * @description
 * # MainCtrl
 * Controller of the webappApp
 */
angular.module('webappApp')
  .controller('MainCtrl', ["$scope", "Organization", "User", function ($scope, Organization, User) {


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
      Organization.all("milestones").all('current').getList().then(function (data) {
        $scope.milestones = data.plain().map(function (m) {
          m.current = true;
          return m;
        });
      })
      Organization.all('issues').customGET("", {q: $scope.query, page: $scope.page}).then(function (data) {
        $scope.issues = data.content;
      });
      function loadBoard() {

        if ($scope.isMember) {
          var assignee = $scope.issue.assignee ? $scope.issue.assignee.name : "";
          var assigneeFilter = assignee == "" ? "" : 'assignee:' + assignee;
          var milestone = $scope.issue.milestone ? "milestone:\"" + $scope.issue.milestone.title + "\"" : "milestone:_current";
          $scope.queryBacklog = 'is:open ' + assigneeFilter + " !label:\"in progress\" " + milestone + " sort:dueTime-asc sort:priority-desc";
          Organization.all('board').all("issues").customGET("", {
            q: $scope.queryBacklog,
            page: $scope.page
          }).then(function (data) {
            $scope.backlogs = data.content;
          });
          milestone = $scope.issue.milestone ? "milestone:\"" + $scope.issue.milestone.title + "\"" : "";
          $scope.queryProgress = 'is:open ' + assigneeFilter + " label:\"in progress\" " + milestone + " sort:dueTime-asc sort:priority-desc ";
          Organization.all('board').all("issues").customGET("", {
            q: $scope.queryProgress,
            page: $scope.page
          }).then(function (data) {
            $scope.inProgress = data.content;
          });

          $scope.queryZombies = 'is:open no:assignee sort:createdAt-desc'
          Organization.all('board').all("issues").customGET("", {
            q: $scope.queryZombies,
            page: $scope.page,
            per_page: 6
          }).then(function (data) {
            $scope.zombies = data.content;
          });
        }
        if ($scope.isMember || $scope.isSupport) {

          $scope.queryClient = 'is:open has:client sort:dueTime-asc sort:priority-desc '
          Organization.all('board').all("issues").customGET("", {
            q: $scope.queryClient,
            page: $scope.page,
            per_page: 6
          }).then(function (data) {
            $scope.clientIssues = data.content;
          });
        }
        if ($scope.isSupport) {
          $scope.queryClient = 'is:open has:client client:_my sort:dueTime-asc sort:priority-desc '
          Organization.all('board').all("issues").customGET("", {
            q: $scope.queryClient,
            page: $scope.page,
            per_page: 6
          }).then(function (data) {
            $scope.myClientIssues = data.content;
          });
        }
      }

      $scope.isMember = User.isMember(ORGANIZATION);
      $scope.isSupport = User.isSupport(ORGANIZATION);


      loadBoard();


      $scope.$on("assignee:changed", function (e, assignee) {

        $scope.issue.assignee = assignee;
        loadBoard();

      });
      $scope.$on("milestone:changed", function (e, m) {
        $scope.issue.milestone = m;
        loadBoard();
      });
    });


  }]);

'use strict';

/**
 * @ngdoc function
 * @name webappApp.controller:MainCtrl
 * @description
 * # MainCtrl
 * Controller of the webappApp
 */
angular.module('webappApp')
    .controller('ClientCtrl', ["$scope", "Organization", function ($scope, Organization) {

        Organization.all("clients").getList().then(function (data) {
            $scope.clients = data.plain();
        })
    }]);

angular.module('webappApp')
    .controller('ClientNewCtrl', ["$scope", "Organization", "$location", function ($scope, Organization, $location) {

        $scope.save = function () {
            Organization.all("clients").post($scope.client).then(function (data) {
                $location.path("/clients/" + $scope.client.clientId);
            })
        }
    }]);

angular.module('webappApp')
    .controller('ClientEditCtrl', ["$scope", "Organization", "$routeParams", "$location", "Notification", function ($scope, Organization, $routeParams, $location, Notification) {


        $scope.contractEditing = false;
        Organization.all("clients").one($routeParams.id).get().then(function (data) {
            $scope.client = data.plain();
        }).catch(function (error, status) {
            if (error.status == 404) {
                $location.path("/");
            }
        })
        Organization.all("clients").one($routeParams.id).all("members").getList().then(function (data) {
            $scope.members = data.plain();
        })
        Organization.all("clients").one($routeParams.id).all("environments").getList().then(function (data) {
            $scope.environments = data.plain();
        })

        Organization.all("clients").one($routeParams.id).all("contracts").getList().then(function (data) {
            $scope.contracts = data.plain();
        })
        Organization.all("contracts").getList().then(function (data) {
            $scope.contractsTypes = data.plain();
        })
        $scope.save = function () {
            Organization.all("clients").one($routeParams.id).patch($scope.client).then(function (data) {
                Notification.success("Client updated successfully.");
            })
        }

        $scope.saveContract = function () {

            Organization.all("clients").one($routeParams.id).all("contracts").post($scope.selectedContract).then(function (data) {
                $scope.contracts.push(data.plain());
                $scope.selectedContract = null;
                $scope.cancelContract();
            })
        }
        $scope.addContract = function () {
            $scope.contractEditing = true;
        }
        $scope.cancelContract = function () {
            $scope.contractEditing = false;
        }
        $scope.createChat = function () {
            Organization.all("clients").one($routeParams.id).all("room").post().then(function (data) {
                console.log("chat created")
            });
        }
        $scope.addMember = function () {
            Organization.all("clients").one($routeParams.id).all("members").one($scope.newMember).post().then(function (data) {
                $scope.members.push(data);
            });
        }
    }]);

'use strict';

/**
 * @ngdoc function
 * @name webappApp.controller:AboutCtrl
 * @description
 * # AboutCtrl
 * Controller of the webappApp
 */
angular.module('webappApp')
  .controller('AboutCtrl', ["$scope", function ($scope) {
    $scope.awesomeThings = [
      'HTML5 Boilerplate',
      'AngularJS',
      'Karma'
    ];
  }]);

'use strict';
angular.module('webappApp')
  .controller('HeaderCtrl', ["$scope", "User", "BreadCrumb", "$location", "$rootScope", function ($scope, User, BreadCrumb, $location, $rootScope) {

    $scope.menuClass = "";
    $scope.breadCrumb = BreadCrumb;
    User.whoami().then(function (user) {
      $scope.user = user;
      $scope.member = User.isMember(ORGANIZATION);
      $scope.isClient = User.isClient(ORGANIZATION);


    })

    $scope.closeMe = function () {
      $('#myNavmenu').offcanvas('toggle');
    }
    $scope.toggleMenu = function () {
      $scope.menuClass = $scope.menuClass == "" ? "show-menu" : "";
    }

    $rootScope.$on('$routeChangeSuccess', function (scope, next, current) {


      if (next.$$route.originalPath.indexOf('/rooms') != -1) {
        $scope.isChat = true;
      } else {
        $scope.isChat = false;
      }
      if (!$scope.user) {
        $scope.$watch('user', function (user) {
          if (user && !user.confirmed) {
            if (!User.isMember(ORGANIZATION) && User.isClient(ORGANIZATION)) {
              $location.path('/users/' + user.name)
            }
          }
        })
      } else {
        if (!$scope.user.confirmed) {
          if (!User.isMember(ORGANIZATION) && User.isClient(ORGANIZATION)) {
            $location.path('/users/' + $scope.user.name)
          }
        }
      }
    })

  }]);

'use strict';
angular.module('webappApp')
  .controller('IssueCtrl', ["$scope", "Organization", "User", "$routeParams", "$location", function ($scope, Organization, User, $routeParams, $location) {

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
  }]
);

angular.module('webappApp')
  .controller('IssueNewCtrl', ["$scope", "Organization", "Repo", "$location", "User", function ($scope, Organization, Repo, $location, User) {

    $scope.carriage = true;

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
  }]);
angular.module('webappApp')
  .controller('IssueEditCtrl', ["$scope", "$routeParams", "Organization", "Repo", "$popover", "$route", "User", "$timeout", "$location", "$q", function ($scope, $routeParams, Organization, Repo, $popover, $route, User, $timeout, $location, $q) {


    $scope.carriage = true;
    $scope.githubIssue = GITHUB + "/" + ORGANIZATION;

    var waiting_reply = 'waiting reply';
    var in_progress = 'in progress';
    $scope.number = $routeParams.id;
    var number = $scope.number;
    User.whoami().then(function (data) {


      $scope.isMember = User.isMember(ORGANIZATION);
      $scope.isSupport = User.isSupport(ORGANIZATION);
      $scope.currentUser = data;

      if ($scope.isMember || $scope.isSupport) {
        Organization.all("clients").getList().then(function (data) {
          $scope.clients = data.plain();
        })
      }
    });
    Organization.all("issues").one(number).get().then(function (data) {
      $scope.issue = data.plain();
      $scope.repo = $scope.issue.repository.name;
      $scope.githubCommit = GITHUB + "/" + ORGANIZATION + "/" + $scope.repo + "/commit/";
      User.whoami().then(function (data) {
        $scope.isOwner = $scope.issue.user.name == data.name;
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
    $scope.close = function () {


      Repo.one($scope.repo).all("issues").one(number).patch({state: "closed"}).then(function (data) {
        $scope.issue.state = "CLOSED";
        if ($scope.newComment && $scope.newComment.body) {
          $scope.comment();
        }
        refreshEvents();
        $scope.removeLabel(in_progress);
      });

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
  }]);

angular.module('webappApp')
  .controller('ChangeLabelCtrl', ["$scope", "$filter", function ($scope, $filter) {

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
  }]);

angular.module('webappApp')
  .controller('ChangeTagCtrl', ["$scope", "$filter", function ($scope, $filter) {

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
  }]);

angular.module('webappApp')
  .controller('ChangeMilestoneCtrl', ["$scope", "$filter", "$routeParams", "Repo", "$popover", function ($scope, $filter, $routeParams, Repo, $popover) {

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
  }]);
angular.module('webappApp')
  .controller('ChangeVersionCtrl', ["$scope", "$filter", function ($scope, $filter) {

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
  }]);
angular.module('webappApp')
  .controller('ChangeAssigneeCtrl', ["$scope", "$filter", function ($scope, $filter) {
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
  }]);

angular.module('webappApp')
  .controller('ChangePriorityCtrl', ["$scope", "$filter", function ($scope, $filter) {

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
  }]);

angular.module('webappApp')
  .controller('ChangeScopeCtrl', ["$scope", "$filter", function ($scope, $filter) {
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
  }]);
angular.module('webappApp')
  .controller('ChangeRepoCtrl', ["$scope", "$filter", function ($scope, $filter) {
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

  }]);
angular.module('webappApp')
  .controller('ChangeClientCtrl', ["$scope", "$filter", function ($scope, $filter) {


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
  }]);

angular.module('webappApp')
  .controller('ChangeSortCtrl', ["$scope", function ($scope) {


    $scope.isSortSelected = function (sort) {
      return $scope.issue.sort ? sort.name == $scope.issue.sort.name : false;
    }
    $scope.toggleSort = function (sort) {
      if (!$scope.isSortSelected(sort)) {
        $scope.$emit("sort:changed", sort);
      }
      $scope.$hide();
    }

  }]);
angular.module('webappApp').controller('CommentController', ["$scope", "Repo", function ($scope, Repo) {
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
}])

angular.module('webappApp').controller('BodyController', ["$scope", "Repo", function ($scope, Repo) {
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
}])

'use strict';

angular.module('webappApp')
  .controller('LoginCtrl', ["$scope", function ($scope) {

  }]);

'use strict';

/**
 * @ngdoc function
 * @name webappApp.controller:AboutCtrl
 * @description
 * # AboutCtrl
 * Controller of the webappApp
 */
angular.module('webappApp')
  .controller('UserCtrl', ["$scope", "User", function ($scope, User) {

    $scope.tabs = [
      {
        title: 'Profile',
        url: 'views/users/profile.html'
      },
      {
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
  }]);

angular.module('webappApp')
  .controller('UserEnvCtrl', ["$scope", "User", "Repo", function ($scope, User, Repo) {


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

  }]);

angular.module('webappApp')
  .controller('ChangeEnvironmentCtrl', ["$scope", "User", "Repo", function ($scope, User, Repo) {

    $scope.environment = angular.copy($scope.issue.environment)

    $scope.connections = ['plocal', 'remote', 'memory']
    Repo.one(DEFAULT_REPO).all("milestones").getList().then(function (data) {
      $scope.milestones = data.plain();
    });

    $scope.save = function () {
      $scope.$emit("environment:changed", $scope.environment);
      $scope.$hide();
    }
  }])
angular.module('webappApp')
  .controller('UserOrgCtrl', ["$scope", "User", "Repo", "Organization", function ($scope, User, Repo, Organization) {

    $scope.areaEditing = false;
    Organization.all("members").getList().then(function (data) {
      $scope.members = data.plain();
    })

    Organization.all("priorities").getList().then(function (data) {
      $scope.priorities = data.plain();
    })

    Organization.all("tags").getList().then(function (data) {
      $scope.tags = data.plain();
    })
    Organization.all("repos").getList().then(function (data) {
      $scope.repositories = data.plain();
      if ($scope.repositories.length > 0) {
        $scope.selectedRepo = $scope.repositories[0];
      }
    })
    Organization.all("scopes").getList().then(function (data) {
      $scope.areas = data.plain();
    })

    Organization.all("bots").getList().then(function (data) {
      $scope.bots = data.plain();
    })

    Organization.all("contracts").getList().then(function (data) {
      $scope.contracts = data.plain();
    })

    Organization.all("milestones").all('current').getList().then(function (data) {
      var currents = data.plain();
      Organization.all("milestones").getList().then(function (data) {
        $scope.milestones = data.plain().map(function (m) {
          currents.forEach(function (m1) {
            if (m1.title == m.title) {
              m.current = true;
            }
          })
          return m;
        });
      })
    })


    $scope.syncRepo = function (repo) {
      Repo.one(repo.name).all("sync").post().then(function (data) {

      })
    }
    $scope.getPriority = function (id) {
      var priority;
      $scope.priorities.forEach(function (p) {

        if (p.number == id) {
          priority = p.name;
        }
      })
      return priority;
    }
    $scope.addBot = function () {
      Organization.all("bots").one($scope.newBot).post().then(function (data) {
        $scope.bots.push(data);
      });
    }
    $scope.addRepository = function () {
      Organization.all("repos").one($scope.newRepo).post().then(function (data) {
        $scope.repositories.push(data.plain());
        $scope.newRepo = null;
      });
    }
    $scope.addArea = function () {
      $scope.areaEditing = true;
      $scope.area = {}
      $scope.area.members = [];
    }
    $scope.addContract = function () {
      $scope.contractEditing = true;
      $scope.contract = {}
      $scope.contract.businessHours = new Array(7);
      $scope.contract.slas = {}
      $scope.priorities.forEach(function (p) {
        $scope.contract.slas[p.number] = 2 * p.number;
      })
    }
    $scope.addTag = function () {
      $scope.tagEditing = true;
      $scope.tag = {}
    }
    $scope.saveMilestone = function (milestone) {
      Organization.all("milestones").one(encodeURI(milestone.title)).patch({current: milestone.current}).then(function (data) {

      })
    }
    $scope.cancelContract = function () {
      $scope.contractEditing = false;
      $scope.contract = {}
      $scope.contract.businessHours = new Array(7);
    }
    $scope.cancelArea = function () {
      $scope.areaEditing = false;
      $scope.area = {}
      $scope.area.members = [];
    }

    $scope.cancelTag = function () {
      $scope.tagEditing = false;
      $scope.tag = {};
    }

    $scope.saveTag = function () {

      if (!$scope.tag.id) {
        Organization.all("tags").post($scope.tag).then(function (data) {
          $scope.tags.push(data);
          $scope.cancelTag();
        })
      } else {
        var idx = $scope.tags.indexOf($scope.selectedTag);
        Organization.all("tags").one($scope.tag.uuid).patch($scope.tag).then(function (data) {
          $scope.tags[idx] = data;
          $scope.cancelTag();
        })
      }
    }
    $scope.saveContract = function () {

      if (!$scope.contract.id) {
        Organization.all("contracts").post($scope.contract).then(function (data) {
          $scope.contracts.push(data);
          $scope.cancelContract();
        })
      } else {
        var idx = $scope.contracts.indexOf($scope.selectedContract);
        Organization.all("contracts").one($scope.contract.uuid).patch($scope.contract).then(function (data) {
          $scope.contracts[idx] = data;
          $scope.cancelContract();
        })
      }
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
    $scope.selectContract = function (contract) {
      $scope.selectedContract = contract;
      $scope.contract = angular.copy(contract);
      $scope.contractEditing = true;
    }

    $scope.selectTag = function (tag) {
      $scope.selectedTag = tag;
      $scope.tag = angular.copy(tag);
      $scope.tagEditing = true;
    }
  }]);


angular.module('webappApp')
  .controller('UserProfileCtrl', ["$scope", "User", "Repo", function ($scope, User, Repo) {


    $scope.message = 'Please fill this information to use PrjHub';
    User.whoami().then(function (user) {
      $scope.user = user;

      $scope.member = User.isMember(ORGANIZATION);
      $scope.isClient = User.isClient(ORGANIZATION);
      $scope.viewMessage = !$scope.user.confirmed && !$scope.member && !$scope.isClient;
    });

    $scope.save = function () {

      User.save($scope.user).then(function (data) {

        var jacked = humane.create({baseCls: 'humane-jackedup', addnCls: 'humane-jackedup-success'})
        jacked.log("Profile saved.");
      });
    }
  }])
angular.module('webappApp')
  .controller('ChangeSelectEnvironmentCtrl', ["$scope", "User", "Repo", function ($scope, User, Repo) {


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
  }])

'use strict';

/**
 * @ngdoc function
 * @name webappApp.controller:AboutCtrl
 * @description
 * # AboutCtrl
 * Controller of the webappApp
 */
angular.module('webappApp')
  .controller('ChatCtrl', ["$scope", "Organization", "$routeParams", "$route", "User", "$timeout", "BreadCrumb", "$location", "ChatService", "$rootScope", "$filter", function ($scope, Organization, $routeParams, $route, User, $timeout, BreadCrumb, $location, ChatService, $rootScope, $filter) {

    $scope.isNew = false;
    $scope.placeholder = "Click here to type a message. Enter to send.";
    $scope.clientId = $routeParams.id;

    $scope.sending = false;

    $scope.connected = ChatService.connected;




    $scope.closeMe = function () {
      $('#roomsMobile').offcanvas('toggle');
    }
    $rootScope.$on('msg-received', function (e, msg) {


      if (msg.sender.name != $scope.currentUser.name) {

        if (msg.edited) {

          if ($scope.clientId == msg.clientId) {
            replaceMsg(msg);
            visit();
          }
        } else {
          if ($scope.clientId == msg.clientId) {
            $scope.$apply(function () {
              addNewMessage(msg);
              visit()

            });
          } else {
            $scope.$apply(function () {
              $scope.clients.forEach(function (c) {
                if (c.clientId == msg.clientId) {
                  c.timestamp = new Date().getTime();
                }
              })
            });
          }
        }
      }
    });


    $scope.loadMore = function () {
      Organization.all("clients").one($scope.clientId).all("room").customGET("", {
        before: $scope.messages[0].messages[0].id
      }).then(function (data) {
        var msg = aggregateMessage(data.reverse());
        msg.reverse();
        msg.forEach(function (e) {
          $scope.messages.unshift(e);
        })
      }).catch(function (e) {

      });
    }


    var findGroup = function (groups, date) {
      return groups.filter(function (g) {
        return g.date == date;
      })
    }
    var visit = function () {
      Organization.all("clients").one($scope.clientId).all("room").all('checkin').patch().then(function (data) {
        $scope.clients.forEach(function (c) {
          if (c.clientId == $scope.clientId) {
            c.lastVisit = new Date().getTime();
          }
        });
      }).catch(function (e) {

      })
    }
    var aggregateMessage = function (msg) {
      var newMsg = []
      var lastTime = null;
      var lastUser = null
      msg.forEach(function (m) {

        if (lastUser == m.sender.name) {
          var momentLast = moment(lastTime);
          var momentCurrent = moment(m.date);
          var diff = momentCurrent.diff(momentLast, "minutes");
          if (diff < 20) {
            var group = findGroup(newMsg, lastTime);
            group[0].messages.push(m);
          } else {
            lastUser = m.sender.name;
            lastTime = m.date;
            newMsg.push({
              sender: m.sender,
              date: lastTime,
              messages: [m]
            })
          }

        } else {
          lastUser = m.sender.name;
          lastTime = m.date;
          newMsg.push({
            sender: m.sender,
            date: lastTime,
            messages: [m]
          })
        }
      });
      return newMsg;
    }

    var replaceMsg = function (msg) {
      $scope.messages.forEach(function (g) {
        g.messages.forEach(function (m) {
          if (m.id === msg.id) {
            m.body = msg.body;
          }
        });
      })
    }
    var addNewMessage = function (message) {


      var len = $scope.messages.length;
      if (len > 0) {
        var lastTime = $scope.messages[len - 1].date;
        var lastGroup = $scope.messages[len - 1];
        if (lastGroup.sender.name == message.sender.name) {

          var momentLast = moment(new Date(parseInt(lastTime)));
          var momentCurrent = moment(new Date(message.date));
          var diff = momentCurrent.diff(momentLast, "minutes");
          if (diff < 20) {
            lastGroup.messages.push(message);
          } else {
            $scope.messages.push({
              date: message.date,
              sender: message.sender,
              messages: [message]
            })
          }
        } else {
          $scope.messages.push({
            date: message.date,
            sender: message.sender,
            messages: [message]
          })
        }
      } else {
        $scope.messages.push({
          date: message.date,
          sender: message.sender,
          messages: [message]
        })
      }
    }
    User.whoami().then(function (data) {
      $scope.currentUser = data;
      $scope.$watch(function () {
        return ChatService.clients;
      }, function (clients) {
        if (clients.length > 0) {
          $scope.clients = clients;
          if (!$scope.clientId) {

            $scope.clients = $filter('orderBy')($scope.clients, "timestamp", true);
            $scope.clientId = $scope.clients[0].clientId.toString();
            $scope.client = $scope.clients[0];
          } else {

            $scope.clients.forEach(function (c) {
              if (c.clientId == $scope.clientId) {
                $scope.client = c;
              }
            })
          }
          BreadCrumb.title = 'Room ' + $scope.client.name;
          getMessages();
        }
      })


      function getMessages() {
        if ($scope.clientId) {
          Organization.all("clients").one($scope.clientId).all("room").getList().then(function (data) {
            $scope.messages = aggregateMessage(data.plain().reverse())
          }).catch(function (e) {
            if (e.status == 400 && e.data) {
              $scope.isNew = true;
            }
          });
          Organization.all("clients").one($scope.clientId).all("room").all('actors').getList().then(function (data) {
            $scope.actors = data.plain();
          });
          visit()
        }
      }

    });


    $scope.createChat = function () {
      Organization.all("clients").one($scope.clientId).all("room").post().then(function (data) {
        var jacked = humane.create({baseCls: 'humane-jackedup', addnCls: 'humane-jackedup-success'})
        jacked.log("Room created");
        $route.reload();
      });
    }
    $scope.sendMessage = function () {
      $scope.sending = true;

      var last = angular.copy($scope.current);
      $scope.current = null;
      $scope.$apply();
      if (last != null) {
        Organization.all("clients").one($scope.clientId).all("room").patch({body: last}).then(function (data) {
          //$scope.current = null;
          addNewMessage(data);
          $scope.sending = false;
          visit();
        }).catch(function () {
          $scope.current = last;
          $scope.sending = false;
        })
      }
    }
  }]);


angular.module('webappApp').controller('MessageController', ["$scope", "Organization", function ($scope, Organization) {


  $scope.placeholder = "This message was deleted";


  $scope.owner = $scope.message.sender.name === $scope.currentUser.name;
  $scope.edit = function () {
    $scope.preview = false;
  }

  $scope.timeToChange = function (message) {
    var input = new Date(parseInt(message.date));

    var then = moment(input);

    var now = moment(new Date());

    var difference = moment.duration(now.diff(then))
    return difference._data.minutes < 10;

  }
  $scope.delete = function () {
    $scope.message.body = null;
    $scope.patchMessage();
  }
  $scope.patchMessage = function () {
    Organization.all("clients").one($scope.clientId).all("room").one(encodeURI($scope.message.id.replace("#", ""))).patch({body: $scope.message.body}).then(function (data) {
      $scope.preview = true
    }).catch(function () {
      $scope.preview = true;
    })
  }
}]);

angular.module('webappApp').controller('TopicCtrl', ["$scope", "$location", "$routeParams", "Organization", "User", function ($scope, $location, $routeParams, Organization, User) {


  $scope.page = 1;
  $scope.query = ""
  $scope.queryText = "";
  if ($routeParams.q) {
    $scope.queryText = $routeParams.q;
    var match = $scope.queryText.match(/(?:[^\s"]+|"[^"]*")+/g);

    match.forEach(function (m) {
      var splitted = m.split(":")
      if (splitted[0] == "text") {
        $scope.query = splitted[1].replace(/"/g, "")
      }
    });

  }
  if ($routeParams.page) {
    $scope.page = $routeParams.page;
  }

  User.whoami().then(function (data) {
    $scope.isMember = User.isMember(ORGANIZATION);
    $scope.isSupport = User.isSupport(ORGANIZATION);
  })
  $scope.searchQuestions = function () {
    Organization.all('topics').customGET("", {q: $scope.queryText, page: $scope.page}).then(function (data) {
      $scope.topics = data.content;
      $scope.pager = data.page;
      $scope.pager.pages = $scope.calculatePages($scope.pager);
    });
  }


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

  $scope.changePage = function (val) {
    if (val > 0 && val <= $scope.pager.totalPages) {
      $scope.page = val;
      $scope.search(true);
    }
  }
  $scope.search = function (page) {
    if (!page) {
      $scope.page = 1;
    }
    var text = "text:\"" + $scope.query + "\"";

    if (!$scope.query || $scope.query == "") {
      text = ""
    }
    $location.search({'q': text, 'page': $scope.page});
  }
  $scope.clear = function () {
    $scope.query = "";
    $scope.search();
  }
  $scope.searchQuestions();
}])

angular.module('webappApp').controller('TopicNewCtrl', ["$scope", "$location", "Organization", "User", function ($scope, $location, Organization, User) {

  $scope.preview = false;
  $scope.carriage = true;
  $scope.topic = {}
  User.whoami().then(function (data) {
    $scope.user = data;
    $scope.isMember = User.isMember(ORGANIZATION);
    $scope.isClient = User.isClient(ORGANIZATION);
    $scope.client = User.getClient(ORGANIZATION);
  });

  Organization.all("tags").getList().then(function (data) {
    $scope.tags = data.plain();
  })
  $scope.save = function () {
    Organization.all("topics").post($scope.topic).then(function (data) {
      $location.path("/topics/" + data.number);
    })
  }
}]);

angular.module('webappApp').controller('TopicEditCtrl', ["$scope", "$location", "$routeParams", "Organization", "User", function ($scope, $location, $routeParams, Organization, User) {


  $scope.carriage = true;
  $scope.number = $routeParams.id;

  $scope.newComment = {};
  User.whoami().then(function (data) {
    $scope.isMember = User.isMember(ORGANIZATION);
    $scope.currentUser = data;

    if ($scope.isMember) {
      Organization.all("clients").getList().then(function (data) {
        $scope.clients = data.plain();
      })
    }
  });

  Organization.all("tags").getList().then(function (data) {
    $scope.tags = data.plain();
  })
  Organization.all("topics").one($scope.number).get().then(function (data) {
    $scope.topic = data.plain();
  })

  Organization.all("topics").one($scope.number).all("comments").getList().then(function (data) {
    $scope.comments = data.plain();
  })

  $scope.comment = function () {
    Organization.all("topics").one($scope.number).all("comments").post($scope.newComment).then(function (data) {
      $scope.comments.push(data);
      $scope.newComment = {}
    })
  }
  $scope.changeTitle = function (title) {
    Organization.all("topics").one($scope.number).patch({title: title}).then(function (data) {
      $scope.topic.title = title;
      $scope.newTitle = null;
      $scope.editingTitle = false;
    });
  }

  $scope.$on("tag:added", function (e, tag) {


    Organization.all("topics").one($scope.number).all("tags").post([tag]).then(function (data) {
      $scope.topic.tags.push(tag);
    })

  })
  $scope.$on("tag:removed", function (e, tag) {
    Organization.all("topics").one($scope.number).all("tags").one(tag.uuid).remove().then(function (data) {
      var idx = $scope.topic.tags.indexOf(tag);
      $scope.topic.tags.splice(idx, 1);
    })
  })
}]);

angular.module('webappApp').controller('TopicBodyController', ["$scope", "Repo", "Organization", function ($scope, Repo, Organization) {
  $scope.preview = true;


  $scope.carriage = true;

  $scope.clonedComment = {};
  $scope.cancelEditing = function () {
    $scope.preview = true;
    $scope.topic.body = $scope.clonedComment;
  }
  $scope.edit = function () {
    $scope.preview = false;
    $scope.clonedComment = angular.copy($scope.topic.body);
  }
  $scope.patchComment = function () {
    Organization.all("topics").one($scope.number).patch({body: $scope.topic.body}).then(function (data) {
      $scope.preview = true;
    }).catch(function () {
      $scope.cancelEditing()
    });
  }
}])


angular.module('webappApp').controller('TopicCommentController', ["$scope", "Organization", function ($scope, Organization) {
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

    Organization.all("topics").one($scope.number).all("comments").one($scope.comment.uuid).remove().then(function (data) {
      var idx = $scope.comments.indexOf($scope.comment);
      if (idx > -1) {
        $scope.comments.splice(idx, 1);
      }
    }).catch(function () {

    });
  }
  $scope.patchComment = function () {
    Organization.all("topics").one($scope.number).all("comments").one($scope.comment.uuid).patch($scope.comment).then(function (data) {
      $scope.preview = true;
    }).catch(function () {
      $scope.cancelEditing()
    });
  }
}])

'use strict';


angular.module('webappApp').factory("User", ["Restangular", "$q", function (Restangular, $q) {

  var userService = Restangular.all('user');
  var allUserService = Restangular.all('users');
  return {

    current: {},

    isClient: function (org) {

      var found = false;
      if (this.current.clientsOf) {

        this.current.clientsOf.forEach(function (o) {
          if (org == o.name) {
            found = true;
          }
        });
      }
      return found;
    },
    getClient: function (org) {

      if (this.current.clients) {
        return this.current.clients[0];
      }
      return null;
    },

    allow: function (org, permission) {
      return this.isMember(org)
    },
    isMember: function (repo) {
      var found = false
      this.current.repositories.forEach(function (e) {
        if (e.organization.name == repo) {
          found = true;
        }
      })
      return found;
    },
    isSupport: function (org) {
      var client = this.getClient(org);
      return this.isClient(org) && client.support;
    },
    whoami: function () {
      var deferred = $q.defer();
      var self = this;
      if (!self.current.name) {
        userService.customGET().then(function (data) {
          self.current = data;
          deferred.resolve(data);
        });
      } else {
        deferred.resolve(self.current);
      }
      return deferred.promise;
    },
    save: function (user) {
      var deferred = $q.defer();
      var self = this;
      allUserService.one(user.name).patch(user).then(function (data) {
        self.current = data;
        deferred.resolve(data);
      });
      return deferred.promise;
    },
    environments: function () {
      var deferred = $q.defer();
      allUserService.one(this.current.name).all('environments').getList().then(function (data) {
        deferred.resolve(data);
      })
      return deferred.promise;
    },
    addEnvironment: function (env) {
      var deferred = $q.defer();
      allUserService.one(this.current.name).all('environments').post(env).then(function (data) {
        deferred.resolve(data);
      })
      return deferred.promise;
    },
    deleteEnvironment: function (env) {
      var deferred = $q.defer();
      allUserService.one(this.current.name).all('environments').one(env.eid.toString()).remove().then(function (data) {
        deferred.resolve(data);
      })
      return deferred.promise;
    },
    changeEnvironment: function (env) {
      var deferred = $q.defer();
      allUserService.one(this.current.name).all('environments').one(env.eid.toString()).patch(env).then(function (data) {
        deferred.resolve(data);
      })
      return deferred.promise;
    }

  }
}]);

angular.module('webappApp').service("AccessToken", ["$localStorage", function ($localStorage) {

  return {
    get: function () {
      return $localStorage.token;
    },
    set: function (token) {
      $localStorage.token = token;
    },
    delete: function () {
      delete  $localStorage.token;
    }
  }
}]);

'use strict';


angular.module('webappApp').factory("Issue", ["$resource", function ($resource) {


  var resource = $resource(API + "")
}]);

'use strict';


angular.module('webappApp').factory("Organization", ["Restangular", function (Restangular) {
  return Restangular.service('orgs').one(ORGANIZATION);
}]);

angular.module('webappApp').factory("Notification", ["$rootScope", function ($rootScope) {


    return {
        success: function (data) {
            var jacked = humane.create({baseCls: 'humane-jackedup', addnCls: 'humane-jackedup-success'})
            jacked.log(data);
        }
    };
}]);
'use strict';


angular.module('webappApp').factory("Repo", ["Restangular", function (Restangular) {
  return Restangular.service('repos').one(ORGANIZATION);
}]);




'use strict';


angular.module('webappApp').factory("BreadCrumb", ["$rootScope", function ($rootScope) {

  var header = {
    title: ""
  }
  $rootScope.$on('$routeChangeStart', function (next, current) {
    header.title = '';
  });
  return header;
}]);




'use strict';


angular.module('webappApp').factory("ChatService", ["$rootScope", "$location", "$timeout", "$window", "User", "Organization", "AccessToken", function ($rootScope, $location, $timeout, $window, User, Organization,AccessToken) {


  var favicon = new Favico({
    animation: 'popFade'
  });

  var charSocketWrapper = {
    socket: null,
    init: function (callback) {

      try {
        this.socket = new WebSocket(WEBSOCKET);
      } catch (err) {
        console.log(err);
        return;
      }
      callback();
    },
    send: function (msg) {
      this.socket.send(msg);
    },
    deinit: function () {
      this.socket.close();
      this.socket = null;
    }
  }
  var chatService = {
    connected: false,
    polling: false,
    clients: [],
    badge: 0,
    clean: function () {
      this.badge = 0;
      favicon.badge(0);
    },
    notify: function (msg) {
      if (!("Notification" in window)) {
        alert("This browser does not support desktop notification");
      }

      // Let's check if the user is okay to get some notification
      else if (Notification.permission === "granted") {
        // If it's okay let's create a notification
        var notification = new Notification("Room " + this.getClientName(msg.clientId), {body: msg.sender.name + ": " + msg.body});
        notification.onclick = function () {
          $location.path('rooms/' + msg.clientId);
        }
      }

      // Otherwise, we need to ask the user for permission
      // Note, Chrome does not implement the permission static property
      // So we have to check for NOT 'denied' instead of 'default'
      else if (Notification.permission !== 'denied') {
        Notification.requestPermission(function (permission) {
          // If the user is okay, let's create a notification
          if (permission === "granted") {

            var notification = new Notification("Room " + this.getClientName(msg.clientId), {body: msg.body});
          }
        });
      }
    },
    send: function (msg) {
      charSocketWrapper.send(msg);
    },

    connect: function () {

      charSocketWrapper.init(initializer)
      this.polling = true;
    },
    disconnect: function () {
      charSocketWrapper.deinit();
      this.polling = false;
    },
    getClientName: function (clientId) {
      var name = ''
      this.clients.forEach(function (c) {
        if (c.clientId == clientId) name = c.name;
      });
      return name;
    }

  }

  var poll = function () {
    $timeout(function () {
      if (!chatService.connected && chatService.polling) {
        console.log("Reconnecting to chat service! ")
        charSocketWrapper.init(initializer)
      }
      poll();
    }, 10000);
  }

  $window.onfocus = function () {
    chatService.clean();
  }
  function initializer() {

    charSocketWrapper.socket.onopen = function () {
      console.log("Connected to chat service! ")
      chatService.connected = true;
      User.whoami().then(function (data) {
        chatService.currentUser = data;
        Organization.all("rooms").getList().then(function (data) {
          chatService.clients = data.plain();
          var msg = {
            "action": "join",
            "organization"  : ORGANIZATION,
            "token" : AccessToken.get(),
            "rooms": []
          }
          chatService.clients.forEach(function (c) {
            if (!c.timestamp) c.timestamp = 0;
            msg.rooms.push(c.clientId);
          })
          chatService.send(JSON.stringify(msg));
        });
      })
    };
    charSocketWrapper.socket.onmessage = function (evt) {
      var msg = JSON.parse(evt.data);
      if (msg.sender.name != chatService.currentUser.name) {
        if (!msg.edited)
          chatService.notify(msg);
        chatService.badge += 1;
        favicon.badge(chatService.badge);
        $rootScope.$broadcast('msg-received', msg);
      }
    };
    charSocketWrapper.socket.onclose = function () {
      console.log("Disconnected from chat service!")
      chatService.connected = false;
    };

    //poll();
  }


  return chatService;
}]).run(["ChatService", function (ChatService) {

}]);




angular.module('ngMoment', []).filter('fromNow', function () {

  return function (input, args) {
    if (!(input instanceof Date)) {
      input = new Date(parseInt(input));
    }
    return moment(input).fromNow();
  };
}).filter('formatDate',function(){
  return function (input, args) {
    if (!(input instanceof Date)) {
      input = new Date(parseInt(input));
    }
    return moment(input).format('MMMM Do YYYY, H:mm');;
  };
});

angular.module('ngUtilFilters', []).filter('toRgbString', function () {

  return function hexToRgb(hex, alpha) {
    var bigint = parseInt(hex, 16);
    var r = (bigint >> 16) & 255;
    var g = (bigint >> 8) & 255;
    var b = bigint & 255;

    if (alpha) {
      return "rgba(" + r + "," + g + "," + b + "," + alpha + ")";
    } else {
      return "rgb(" + r + "," + g + "," + b + ")";
    }
  }
});


'use strict';

angular.module('webappApp').directive('mkeditor', ["$timeout", function ($timeout) {
  return {
    require: '^ngModel',
    scope: {
      readOnly: "=readOnly"
    },
    link: function (scope, elem, attrs, ngModel) {


      var editor;
      scope.$watch(function () {
        return ngModel.$modelValue;
      }, initialize);

      function initialize(value) {
        if (value) {
          ngModel.$setViewValue(value);

        }
        if (!editor) {
          $(elem).markdown({
            autofocus: false,
            savable: false,
            onShow: function (e) {

              e.setContent(value);
              //e.showPreview();
              editor = e;
            },
            onChange: function (e) {
              ngModel.$setViewValue(e.getContent());
            }
          })

        } else {
          editor.setContent(value);
        }

      }
    }
  }
}]);

angular.module('webappApp').directive('autofocus', ['$timeout', function ($timeout) {
  return {
    restrict: 'A',
    link: function ($scope, $element) {
      $timeout(function () {
        $element[0].focus();
      });
    }
  }
}]);
angular.module('webappApp').directive('vueEditor', ["$timeout", "$compile", "$http", "$typeahead", function ($timeout, $compile, $http, $typeahead) {
  return {
    require: '^ngModel',
    scope: {
      preview: "=?preview",
      carriage: "=?carriage",
      placeholder: "=?placeholder",
      nullValue: "=?nullValue",
      onSend: '&'
    },
    templateUrl: 'views/vueditor.html',
    controller: ["$scope", function ($scope) {
      if ($scope.preview == undefined) {
        $scope.preview = true
      }
      if($scope.carriage == undefined){
        $scope.carriage = false;
      }
    }],
    link: function (scope, elem, attrs, ngModel) {
      var editor;


      scope.$watch(function () {
        return ngModel.$modelValue;
      }, initialize);

      function initialize(value) {

        ngModel.$setViewValue(value);

        if (!editor) {
          var showing = false;

          scope.$parent.$watch('actors', function (val) {
            if (val) {
              scope.actors = val;

              var text = elem.children()[0];
              $(elem.children()[0]).suggest('@', {
                data: scope.actors,
                map: function (user) {
                  return {
                    value: user.name,
                    text: '<strong>' + user.name + '</strong>'
                  }
                },
                onshow: function (e) {
                  showing = true;
                },
                onselect: function (e) {
                  editor.$data.input = $(text).val()
                  showing = false;
                },
                onhide: function () {
                  showing = false;
                }

              })
            }
          })

          scope.placeholder = scope.placeholder || 'Leave a comment'
          var defaultVal = scope.preview ? 'No description' : '';
          defaultVal = scope.nullValue ? scope.nullValue : defaultVal;
          var elementArea = elem[0];

          $(elementArea).focus();
          editor = new Vue({
            el: elem[0],
            data: {
              input: value || defaultVal
            },
            filters: {
              marked: marked
            },
            methods: {
              send: function (e) {


                if (!scope.carriage && (e.keyCode == 13) && (!e.ctrlKey && !e.shiftKey) && !showing) {
                  e.preventDefault();
                  if (scope.onSend && editor.$data.input && editor.$data.input.length > 0) {
                    scope.onSend();
                  }
                }
              }
            }
          })


          scope.$parent.$watch('sending', function (val) {
            scope.sending = val;


          })
          editor.$watch('$data.input', function (newVal, oldval) {

            ngModel.$setViewValue(newVal);

          });
        } else {
          var defaultVal = scope.preview ? 'No description' : '';
          defaultVal = scope.nullValue ? scope.nullValue : defaultVal;
          editor.$data.input = value || defaultVal
        }

      }
    }
  }
}]);
angular.module('webappApp').directive('avatar', ["$timeout", function ($timeout) {
  return {
    restrict: 'E',
    scope: {
      user: "=user",
      dim: "=dim",
      name: "=name"
    },
    controller: ["$scope", function ($scope) {

    }],
    templateUrl: 'views/avatar.html'
  }
}]);


angular.module('scroll', []).directive('whenScrolled', function () {
  return function (scope, elm, attr) {
    var raw = elm[0];

    elm.bind('scroll', function () {
      if (raw.scrollTop == 0) {
        scope.$apply(attr.whenScrolled);
      }
    });
  };
});


/**
 * the HTML5 autofocus property can be finicky when it comes to dynamically loaded
 * templates and such with AngularJS. Use this simple directive to
 * tame this beast once and for all.
 *
 * Usage:
 * <input type="text" autofocus>
 */
angular.module('utils.autofocus', [])

  .directive('autofocus', ['$timeout', function ($timeout) {
    return {
      restrict: 'A',
      link: function ($scope, $element) {
        $timeout(function () {
          $element[0].focus();
        }, 200);
      }
    }
  }]);

(function ($, undefined) {
  $.fn.getCursorPosition = function () {
    var el = $(this).get(0);
    var pos = 0;
    if ('selectionStart' in el) {
      pos = el.selectionStart;
    } else if ('selection' in document) {
      el.focus();
      var Sel = document.selection.createRange();
      var SelLength = document.selection.createRange().text.length;
      Sel.moveStart('character', -el.value.length);
      pos = Sel.text.length - SelLength;
    }
    return pos;
  }
})(jQuery);

/**
 * marked - a markdown parser
 * Copyright (c) 2011-2014, Christopher Jeffrey. (MIT Licensed)
 * https://github.com/chjj/marked
 */

;(function() {

/**
 * Block-Level Grammar
 */

var block = {
  newline: /^\n+/,
  code: /^( {4}[^\n]+\n*)+/,
  fences: noop,
  hr: /^( *[-*_]){3,} *(?:\n+|$)/,
  heading: /^ *(#{1,6}) *([^\n]+?) *#* *(?:\n+|$)/,
  nptable: noop,
  lheading: /^([^\n]+)\n *(=|-){2,} *(?:\n+|$)/,
  blockquote: /^( *>[^\n]+(\n(?!def)[^\n]+)*\n*)+/,
  list: /^( *)(bull) [\s\S]+?(?:hr|def|\n{2,}(?! )(?!\1bull )\n*|\s*$)/,
  html: /^ *(?:comment|closed|closing) *(?:\n{2,}|\s*$)/,
  def: /^ *\[([^\]]+)\]: *<?([^\s>]+)>?(?: +["(]([^\n]+)[")])? *(?:\n+|$)/,
  table: noop,
  paragraph: /^((?:[^\n]+\n?(?!hr|heading|lheading|blockquote|tag|def))+)\n*/,
  text: /^[^\n]+/
};

block.bullet = /(?:[*+-]|\d+\.)/;
block.item = /^( *)(bull) [^\n]*(?:\n(?!\1bull )[^\n]*)*/;
block.item = replace(block.item, 'gm')
  (/bull/g, block.bullet)
  ();

block.list = replace(block.list)
  (/bull/g, block.bullet)
  ('hr', '\\n+(?=\\1?(?:[-*_] *){3,}(?:\\n+|$))')
  ('def', '\\n+(?=' + block.def.source + ')')
  ();

block.blockquote = replace(block.blockquote)
  ('def', block.def)
  ();

block._tag = '(?!(?:'
  + 'a|em|strong|small|s|cite|q|dfn|abbr|data|time|code'
  + '|var|samp|kbd|sub|sup|i|b|u|mark|ruby|rt|rp|bdi|bdo'
  + '|span|br|wbr|ins|del|img)\\b)\\w+(?!:/|[^\\w\\s@]*@)\\b';

block.html = replace(block.html)
  ('comment', /<!--[\s\S]*?-->/)
  ('closed', /<(tag)[\s\S]+?<\/\1>/)
  ('closing', /<tag(?:"[^"]*"|'[^']*'|[^'">])*?>/)
  (/tag/g, block._tag)
  ();

block.paragraph = replace(block.paragraph)
  ('hr', block.hr)
  ('heading', block.heading)
  ('lheading', block.lheading)
  ('blockquote', block.blockquote)
  ('tag', '<' + block._tag)
  ('def', block.def)
  ();

/**
 * Normal Block Grammar
 */

block.normal = merge({}, block);

/**
 * GFM Block Grammar
 */

block.gfm = merge({}, block.normal, {
  fences: /^ *(`{3,}|~{3,}) *(\S+)? *\n([\s\S]+?)\s*\1 *(?:\n+|$)/,
  paragraph: /^/
});

block.gfm.paragraph = replace(block.paragraph)
  ('(?!', '(?!'
    + block.gfm.fences.source.replace('\\1', '\\2') + '|'
    + block.list.source.replace('\\1', '\\3') + '|')
  ();

/**
 * GFM + Tables Block Grammar
 */

block.tables = merge({}, block.gfm, {
  nptable: /^ *(\S.*\|.*)\n *([-:]+ *\|[-| :]*)\n((?:.*\|.*(?:\n|$))*)\n*/,
  table: /^ *\|(.+)\n *\|( *[-:]+[-| :]*)\n((?: *\|.*(?:\n|$))*)\n*/
});

/**
 * Block Lexer
 */

function Lexer(options) {
  this.tokens = [];
  this.tokens.links = {};
  this.options = options || marked.defaults;
  this.rules = block.normal;

  if (this.options.gfm) {
    if (this.options.tables) {
      this.rules = block.tables;
    } else {
      this.rules = block.gfm;
    }
  }
}

/**
 * Expose Block Rules
 */

Lexer.rules = block;

/**
 * Static Lex Method
 */

Lexer.lex = function(src, options) {
  var lexer = new Lexer(options);
  return lexer.lex(src);
};

/**
 * Preprocessing
 */

Lexer.prototype.lex = function(src) {
  src = src
    .replace(/\r\n|\r/g, '\n')
    .replace(/\t/g, '    ')
    .replace(/\u00a0/g, ' ')
    .replace(/\u2424/g, '\n');

  return this.token(src, true);
};

/**
 * Lexing
 */

Lexer.prototype.token = function(src, top, bq) {
  var src = src.replace(/^ +$/gm, '')
    , next
    , loose
    , cap
    , bull
    , b
    , item
    , space
    , i
    , l;

  while (src) {
    // newline
    if (cap = this.rules.newline.exec(src)) {
      src = src.substring(cap[0].length);
      if (cap[0].length > 1) {
        this.tokens.push({
          type: 'space'
        });
      }
    }

    // code
    if (cap = this.rules.code.exec(src)) {
      src = src.substring(cap[0].length);
      cap = cap[0].replace(/^ {4}/gm, '');
      this.tokens.push({
        type: 'code',
        text: !this.options.pedantic
          ? cap.replace(/\n+$/, '')
          : cap
      });
      continue;
    }

    // fences (gfm)
    if (cap = this.rules.fences.exec(src)) {
      src = src.substring(cap[0].length);
      this.tokens.push({
        type: 'code',
        lang: cap[2],
        text: cap[3]
      });
      continue;
    }

    // heading
    if (cap = this.rules.heading.exec(src)) {
      src = src.substring(cap[0].length);
      this.tokens.push({
        type: 'heading',
        depth: cap[1].length,
        text: cap[2]
      });
      continue;
    }

    // table no leading pipe (gfm)
    if (top && (cap = this.rules.nptable.exec(src))) {
      src = src.substring(cap[0].length);

      item = {
        type: 'table',
        header: cap[1].replace(/^ *| *\| *$/g, '').split(/ *\| */),
        align: cap[2].replace(/^ *|\| *$/g, '').split(/ *\| */),
        cells: cap[3].replace(/\n$/, '').split('\n')
      };

      for (i = 0; i < item.align.length; i++) {
        if (/^ *-+: *$/.test(item.align[i])) {
          item.align[i] = 'right';
        } else if (/^ *:-+: *$/.test(item.align[i])) {
          item.align[i] = 'center';
        } else if (/^ *:-+ *$/.test(item.align[i])) {
          item.align[i] = 'left';
        } else {
          item.align[i] = null;
        }
      }

      for (i = 0; i < item.cells.length; i++) {
        item.cells[i] = item.cells[i].split(/ *\| */);
      }

      this.tokens.push(item);

      continue;
    }

    // lheading
    if (cap = this.rules.lheading.exec(src)) {
      src = src.substring(cap[0].length);
      this.tokens.push({
        type: 'heading',
        depth: cap[2] === '=' ? 1 : 2,
        text: cap[1]
      });
      continue;
    }

    // hr
    if (cap = this.rules.hr.exec(src)) {
      src = src.substring(cap[0].length);
      this.tokens.push({
        type: 'hr'
      });
      continue;
    }

    // blockquote
    if (cap = this.rules.blockquote.exec(src)) {
      src = src.substring(cap[0].length);

      this.tokens.push({
        type: 'blockquote_start'
      });

      cap = cap[0].replace(/^ *> ?/gm, '');

      // Pass `top` to keep the current
      // "toplevel" state. This is exactly
      // how markdown.pl works.
      this.token(cap, top, true);

      this.tokens.push({
        type: 'blockquote_end'
      });

      continue;
    }

    // list
    if (cap = this.rules.list.exec(src)) {
      src = src.substring(cap[0].length);
      bull = cap[2];

      this.tokens.push({
        type: 'list_start',
        ordered: bull.length > 1
      });

      // Get each top-level item.
      cap = cap[0].match(this.rules.item);

      next = false;
      l = cap.length;
      i = 0;

      for (; i < l; i++) {
        item = cap[i];

        // Remove the list item's bullet
        // so it is seen as the next token.
        space = item.length;
        item = item.replace(/^ *([*+-]|\d+\.) +/, '');

        // Outdent whatever the
        // list item contains. Hacky.
        if (~item.indexOf('\n ')) {
          space -= item.length;
          item = !this.options.pedantic
            ? item.replace(new RegExp('^ {1,' + space + '}', 'gm'), '')
            : item.replace(/^ {1,4}/gm, '');
        }

        // Determine whether the next list item belongs here.
        // Backpedal if it does not belong in this list.
        if (this.options.smartLists && i !== l - 1) {
          b = block.bullet.exec(cap[i + 1])[0];
          if (bull !== b && !(bull.length > 1 && b.length > 1)) {
            src = cap.slice(i + 1).join('\n') + src;
            i = l - 1;
          }
        }

        // Determine whether item is loose or not.
        // Use: /(^|\n)(?! )[^\n]+\n\n(?!\s*$)/
        // for discount behavior.
        loose = next || /\n\n(?!\s*$)/.test(item);
        if (i !== l - 1) {
          next = item.charAt(item.length - 1) === '\n';
          if (!loose) loose = next;
        }

        this.tokens.push({
          type: loose
            ? 'loose_item_start'
            : 'list_item_start'
        });

        // Recurse.
        this.token(item, false, bq);

        this.tokens.push({
          type: 'list_item_end'
        });
      }

      this.tokens.push({
        type: 'list_end'
      });

      continue;
    }

    // html
    if (cap = this.rules.html.exec(src)) {
      src = src.substring(cap[0].length);
      this.tokens.push({
        type: this.options.sanitize
          ? 'paragraph'
          : 'html',
        pre: cap[1] === 'pre' || cap[1] === 'script' || cap[1] === 'style',
        text: cap[0]
      });
      continue;
    }

    // def
    if ((!bq && top) && (cap = this.rules.def.exec(src))) {
      src = src.substring(cap[0].length);
      this.tokens.links[cap[1].toLowerCase()] = {
        href: cap[2],
        title: cap[3]
      };
      continue;
    }

    // table (gfm)
    if (top && (cap = this.rules.table.exec(src))) {
      src = src.substring(cap[0].length);

      item = {
        type: 'table',
        header: cap[1].replace(/^ *| *\| *$/g, '').split(/ *\| */),
        align: cap[2].replace(/^ *|\| *$/g, '').split(/ *\| */),
        cells: cap[3].replace(/(?: *\| *)?\n$/, '').split('\n')
      };

      for (i = 0; i < item.align.length; i++) {
        if (/^ *-+: *$/.test(item.align[i])) {
          item.align[i] = 'right';
        } else if (/^ *:-+: *$/.test(item.align[i])) {
          item.align[i] = 'center';
        } else if (/^ *:-+ *$/.test(item.align[i])) {
          item.align[i] = 'left';
        } else {
          item.align[i] = null;
        }
      }

      for (i = 0; i < item.cells.length; i++) {
        item.cells[i] = item.cells[i]
          .replace(/^ *\| *| *\| *$/g, '')
          .split(/ *\| */);
      }

      this.tokens.push(item);

      continue;
    }

    // top-level paragraph
    if (top && (cap = this.rules.paragraph.exec(src))) {
      src = src.substring(cap[0].length);
      this.tokens.push({
        type: 'paragraph',
        text: cap[1].charAt(cap[1].length - 1) === '\n'
          ? cap[1].slice(0, -1)
          : cap[1]
      });
      continue;
    }

    // text
    if (cap = this.rules.text.exec(src)) {
      // Top-level should never reach here.
      src = src.substring(cap[0].length);
      this.tokens.push({
        type: 'text',
        text: cap[0]
      });
      continue;
    }

    if (src) {
      throw new
        Error('Infinite loop on byte: ' + src.charCodeAt(0));
    }
  }

  return this.tokens;
};

/**
 * Inline-Level Grammar
 */

var inline = {
  escape: /^\\([\\`*{}\[\]()#+\-.!_>])/,
  autolink: /^<([^ >]+(@|:\/)[^ >]+)>/,
  url: noop,
  tag: /^<!--[\s\S]*?-->|^<\/?\w+(?:"[^"]*"|'[^']*'|[^'">])*?>/,
  link: /^!?\[(inside)\]\(href\)/,
  reflink: /^!?\[(inside)\]\s*\[([^\]]*)\]/,
  nolink: /^!?\[((?:\[[^\]]*\]|[^\[\]])*)\]/,
  strong: /^__([\s\S]+?)__(?!_)|^\*\*([\s\S]+?)\*\*(?!\*)/,
  em: /^\b_((?:__|[\s\S])+?)_\b|^\*((?:\*\*|[\s\S])+?)\*(?!\*)/,
  code: /^(`+)\s*([\s\S]*?[^`])\s*\1(?!`)/,
  br: /^ {2,}\n(?!\s*$)/,
  del: noop,
  text: /^[\s\S]+?(?=[\\<!\[_*`]| {2,}\n|$)/
};

inline._inside = /(?:\[[^\]]*\]|[^\[\]]|\](?=[^\[]*\]))*/;
inline._href = /\s*<?([\s\S]*?)>?(?:\s+['"]([\s\S]*?)['"])?\s*/;

inline.link = replace(inline.link)
  ('inside', inline._inside)
  ('href', inline._href)
  ();

inline.reflink = replace(inline.reflink)
  ('inside', inline._inside)
  ();

/**
 * Normal Inline Grammar
 */

inline.normal = merge({}, inline);

/**
 * Pedantic Inline Grammar
 */

inline.pedantic = merge({}, inline.normal, {
  strong: /^__(?=\S)([\s\S]*?\S)__(?!_)|^\*\*(?=\S)([\s\S]*?\S)\*\*(?!\*)/,
  em: /^_(?=\S)([\s\S]*?\S)_(?!_)|^\*(?=\S)([\s\S]*?\S)\*(?!\*)/
});

/**
 * GFM Inline Grammar
 */

inline.gfm = merge({}, inline.normal, {
  escape: replace(inline.escape)('])', '~|])')(),
  url: /^(https?:\/\/[^\s<]+[^<.,:;"')\]\s])/,
  del: /^~~(?=\S)([\s\S]*?\S)~~/,
  text: replace(inline.text)
    (']|', '~]|')
    ('|', '|https?://|')
    ()
});

/**
 * GFM + Line Breaks Inline Grammar
 */

inline.breaks = merge({}, inline.gfm, {
  br: replace(inline.br)('{2,}', '*')(),
  text: replace(inline.gfm.text)('{2,}', '*')()
});

/**
 * Inline Lexer & Compiler
 */

function InlineLexer(links, options) {
  this.options = options || marked.defaults;
  this.links = links;
  this.rules = inline.normal;
  this.renderer = this.options.renderer || new Renderer;
  this.renderer.options = this.options;

  if (!this.links) {
    throw new
      Error('Tokens array requires a `links` property.');
  }

  if (this.options.gfm) {
    if (this.options.breaks) {
      this.rules = inline.breaks;
    } else {
      this.rules = inline.gfm;
    }
  } else if (this.options.pedantic) {
    this.rules = inline.pedantic;
  }
}

/**
 * Expose Inline Rules
 */

InlineLexer.rules = inline;

/**
 * Static Lexing/Compiling Method
 */

InlineLexer.output = function(src, links, options) {
  var inline = new InlineLexer(links, options);
  return inline.output(src);
};

/**
 * Lexing/Compiling
 */

InlineLexer.prototype.output = function(src) {
  var out = ''
    , link
    , text
    , href
    , cap;

  while (src) {
    // escape
    if (cap = this.rules.escape.exec(src)) {
      src = src.substring(cap[0].length);
      out += cap[1];
      continue;
    }

    // autolink
    if (cap = this.rules.autolink.exec(src)) {
      src = src.substring(cap[0].length);
      if (cap[2] === '@') {
        text = cap[1].charAt(6) === ':'
          ? this.mangle(cap[1].substring(7))
          : this.mangle(cap[1]);
        href = this.mangle('mailto:') + text;
      } else {
        text = escape(cap[1]);
        href = text;
      }
      out += this.renderer.link(href, null, text);
      continue;
    }

    // url (gfm)
    if (!this.inLink && (cap = this.rules.url.exec(src))) {
      src = src.substring(cap[0].length);
      text = escape(cap[1]);
      href = text;
      out += this.renderer.link(href, null, text);
      continue;
    }

    // tag
    if (cap = this.rules.tag.exec(src)) {
      if (!this.inLink && /^<a /i.test(cap[0])) {
        this.inLink = true;
      } else if (this.inLink && /^<\/a>/i.test(cap[0])) {
        this.inLink = false;
      }
      src = src.substring(cap[0].length);
      out += this.options.sanitize
        ? escape(cap[0])
        : cap[0];
      continue;
    }

    // link
    if (cap = this.rules.link.exec(src)) {
      src = src.substring(cap[0].length);
      this.inLink = true;
      out += this.outputLink(cap, {
        href: cap[2],
        title: cap[3]
      });
      this.inLink = false;
      continue;
    }

    // reflink, nolink
    if ((cap = this.rules.reflink.exec(src))
        || (cap = this.rules.nolink.exec(src))) {
      src = src.substring(cap[0].length);
      link = (cap[2] || cap[1]).replace(/\s+/g, ' ');
      link = this.links[link.toLowerCase()];
      if (!link || !link.href) {
        out += cap[0].charAt(0);
        src = cap[0].substring(1) + src;
        continue;
      }
      this.inLink = true;
      out += this.outputLink(cap, link);
      this.inLink = false;
      continue;
    }

    // strong
    if (cap = this.rules.strong.exec(src)) {
      src = src.substring(cap[0].length);
      out += this.renderer.strong(this.output(cap[2] || cap[1]));
      continue;
    }

    // em
    if (cap = this.rules.em.exec(src)) {
      src = src.substring(cap[0].length);
      out += this.renderer.em(this.output(cap[2] || cap[1]));
      continue;
    }

    // code
    if (cap = this.rules.code.exec(src)) {
      src = src.substring(cap[0].length);
      out += this.renderer.codespan(escape(cap[2], true));
      continue;
    }

    // br
    if (cap = this.rules.br.exec(src)) {
      src = src.substring(cap[0].length);
      out += this.renderer.br();
      continue;
    }

    // del (gfm)
    if (cap = this.rules.del.exec(src)) {
      src = src.substring(cap[0].length);
      out += this.renderer.del(this.output(cap[1]));
      continue;
    }

    // text
    if (cap = this.rules.text.exec(src)) {
      src = src.substring(cap[0].length);
      out += escape(this.smartypants(cap[0]));
      continue;
    }

    if (src) {
      throw new
        Error('Infinite loop on byte: ' + src.charCodeAt(0));
    }
  }

  return out;
};

/**
 * Compile Link
 */

InlineLexer.prototype.outputLink = function(cap, link) {
  var href = escape(link.href)
    , title = link.title ? escape(link.title) : null;

  return cap[0].charAt(0) !== '!'
    ? this.renderer.link(href, title, this.output(cap[1]))
    : this.renderer.image(href, title, escape(cap[1]));
};

/**
 * Smartypants Transformations
 */

InlineLexer.prototype.smartypants = function(text) {
  if (!this.options.smartypants) return text;
  return text
    // em-dashes
    .replace(/--/g, '\u2014')
    // opening singles
    .replace(/(^|[-\u2014/(\[{"\s])'/g, '$1\u2018')
    // closing singles & apostrophes
    .replace(/'/g, '\u2019')
    // opening doubles
    .replace(/(^|[-\u2014/(\[{\u2018\s])"/g, '$1\u201c')
    // closing doubles
    .replace(/"/g, '\u201d')
    // ellipses
    .replace(/\.{3}/g, '\u2026');
};

/**
 * Mangle Links
 */

InlineLexer.prototype.mangle = function(text) {
  var out = ''
    , l = text.length
    , i = 0
    , ch;

  for (; i < l; i++) {
    ch = text.charCodeAt(i);
    if (Math.random() > 0.5) {
      ch = 'x' + ch.toString(16);
    }
    out += '&#' + ch + ';';
  }

  return out;
};

/**
 * Renderer
 */

function Renderer(options) {
  this.options = options || {};
}

Renderer.prototype.code = function(code, lang, escaped) {
  if (this.options.highlight) {
    var out = this.options.highlight(code, lang);
    if (out != null && out !== code) {
      escaped = true;
      code = out;
    }
  }

  if (!lang) {
    return '<pre><code>'
      + (escaped ? code : escape(code, true))
      + '\n</code></pre>';
  }

  return '<pre><code class="'
    + this.options.langPrefix
    + escape(lang, true)
    + '">'
    + (escaped ? code : escape(code, true))
    + '\n</code></pre>\n';
};

Renderer.prototype.blockquote = function(quote) {
  return '<blockquote>\n' + quote + '</blockquote>\n';
};

Renderer.prototype.html = function(html) {
  return html;
};

Renderer.prototype.heading = function(text, level, raw) {
  return '<h'
    + level
    + ' id="'
    + this.options.headerPrefix
    + raw.toLowerCase().replace(/[^\w]+/g, '-')
    + '">'
    + text
    + '</h'
    + level
    + '>\n';
};

Renderer.prototype.hr = function() {
  return this.options.xhtml ? '<hr/>\n' : '<hr>\n';
};

Renderer.prototype.list = function(body, ordered) {
  var type = ordered ? 'ol' : 'ul';
  return '<' + type + '>\n' + body + '</' + type + '>\n';
};

Renderer.prototype.listitem = function(text) {
  return '<li>' + text + '</li>\n';
};

Renderer.prototype.paragraph = function(text) {
  return '<p>' + text + '</p>\n';
};

Renderer.prototype.table = function(header, body) {
  return '<table>\n'
    + '<thead>\n'
    + header
    + '</thead>\n'
    + '<tbody>\n'
    + body
    + '</tbody>\n'
    + '</table>\n';
};

Renderer.prototype.tablerow = function(content) {
  return '<tr>\n' + content + '</tr>\n';
};

Renderer.prototype.tablecell = function(content, flags) {
  var type = flags.header ? 'th' : 'td';
  var tag = flags.align
    ? '<' + type + ' style="text-align:' + flags.align + '">'
    : '<' + type + '>';
  return tag + content + '</' + type + '>\n';
};

// span level renderer
Renderer.prototype.strong = function(text) {
  return '<strong>' + text + '</strong>';
};

Renderer.prototype.em = function(text) {
  return '<em>' + text + '</em>';
};

Renderer.prototype.codespan = function(text) {
  return '<code>' + text + '</code>';
};

Renderer.prototype.br = function() {
  return this.options.xhtml ? '<br/>' : '<br>';
};

Renderer.prototype.del = function(text) {
  return '<del>' + text + '</del>';
};

Renderer.prototype.link = function(href, title, text) {
  if (this.options.sanitize) {
    try {
      var prot = decodeURIComponent(unescape(href))
        .replace(/[^\w:]/g, '')
        .toLowerCase();
    } catch (e) {
      return '';
    }
    if (prot.indexOf('javascript:') === 0) {
      return '';
    }
  }
  var out = '<a href="' + href + '"';
  if (title) {
    out += ' title="' + title + '"';
  }
  out += '>' + text + '</a>';
  return out;
};

Renderer.prototype.image = function(href, title, text) {
  var out = '<img src="' + href + '" alt="' + text + '"';
  if (title) {
    out += ' title="' + title + '"';
  }
  out += this.options.xhtml ? '/>' : '>';
  return out;
};

/**
 * Parsing & Compiling
 */

function Parser(options) {
  this.tokens = [];
  this.token = null;
  this.options = options || marked.defaults;
  this.options.renderer = this.options.renderer || new Renderer;
  this.renderer = this.options.renderer;
  this.renderer.options = this.options;
}

/**
 * Static Parse Method
 */

Parser.parse = function(src, options, renderer) {
  var parser = new Parser(options, renderer);
  return parser.parse(src);
};

/**
 * Parse Loop
 */

Parser.prototype.parse = function(src) {
  this.inline = new InlineLexer(src.links, this.options, this.renderer);
  this.tokens = src.reverse();

  var out = '';
  while (this.next()) {
    out += this.tok();
  }

  return out;
};

/**
 * Next Token
 */

Parser.prototype.next = function() {
  return this.token = this.tokens.pop();
};

/**
 * Preview Next Token
 */

Parser.prototype.peek = function() {
  return this.tokens[this.tokens.length - 1] || 0;
};

/**
 * Parse Text Tokens
 */

Parser.prototype.parseText = function() {
  var body = this.token.text;

  while (this.peek().type === 'text') {
    body += '\n' + this.next().text;
  }

  return this.inline.output(body);
};

/**
 * Parse Current Token
 */

Parser.prototype.tok = function() {
  switch (this.token.type) {
    case 'space': {
      return '';
    }
    case 'hr': {
      return this.renderer.hr();
    }
    case 'heading': {
      return this.renderer.heading(
        this.inline.output(this.token.text),
        this.token.depth,
        this.token.text);
    }
    case 'code': {
      return this.renderer.code(this.token.text,
        this.token.lang,
        this.token.escaped);
    }
    case 'table': {
      var header = ''
        , body = ''
        , i
        , row
        , cell
        , flags
        , j;

      // header
      cell = '';
      for (i = 0; i < this.token.header.length; i++) {
        flags = { header: true, align: this.token.align[i] };
        cell += this.renderer.tablecell(
          this.inline.output(this.token.header[i]),
          { header: true, align: this.token.align[i] }
        );
      }
      header += this.renderer.tablerow(cell);

      for (i = 0; i < this.token.cells.length; i++) {
        row = this.token.cells[i];

        cell = '';
        for (j = 0; j < row.length; j++) {
          cell += this.renderer.tablecell(
            this.inline.output(row[j]),
            { header: false, align: this.token.align[j] }
          );
        }

        body += this.renderer.tablerow(cell);
      }
      return this.renderer.table(header, body);
    }
    case 'blockquote_start': {
      var body = '';

      while (this.next().type !== 'blockquote_end') {
        body += this.tok();
      }

      return this.renderer.blockquote(body);
    }
    case 'list_start': {
      var body = ''
        , ordered = this.token.ordered;

      while (this.next().type !== 'list_end') {
        body += this.tok();
      }

      return this.renderer.list(body, ordered);
    }
    case 'list_item_start': {
      var body = '';

      while (this.next().type !== 'list_item_end') {
        body += this.token.type === 'text'
          ? this.parseText()
          : this.tok();
      }

      return this.renderer.listitem(body);
    }
    case 'loose_item_start': {
      var body = '';

      while (this.next().type !== 'list_item_end') {
        body += this.tok();
      }

      return this.renderer.listitem(body);
    }
    case 'html': {
      var html = !this.token.pre && !this.options.pedantic
        ? this.inline.output(this.token.text)
        : this.token.text;
      return this.renderer.html(html);
    }
    case 'paragraph': {
      return this.renderer.paragraph(this.inline.output(this.token.text));
    }
    case 'text': {
      return this.renderer.paragraph(this.parseText());
    }
  }
};

/**
 * Helpers
 */

function escape(html, encode) {
  return html
    .replace(!encode ? /&(?!#?\w+;)/g : /&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function unescape(html) {
  return html.replace(/&([#\w]+);/g, function(_, n) {
    n = n.toLowerCase();
    if (n === 'colon') return ':';
    if (n.charAt(0) === '#') {
      return n.charAt(1) === 'x'
        ? String.fromCharCode(parseInt(n.substring(2), 16))
        : String.fromCharCode(+n.substring(1));
    }
    return '';
  });
}

function replace(regex, opt) {
  regex = regex.source;
  opt = opt || '';
  return function self(name, val) {
    if (!name) return new RegExp(regex, opt);
    val = val.source || val;
    val = val.replace(/(^|[^\[])\^/g, '$1');
    regex = regex.replace(name, val);
    return self;
  };
}

function noop() {}
noop.exec = noop;

function merge(obj) {
  var i = 1
    , target
    , key;

  for (; i < arguments.length; i++) {
    target = arguments[i];
    for (key in target) {
      if (Object.prototype.hasOwnProperty.call(target, key)) {
        obj[key] = target[key];
      }
    }
  }

  return obj;
}


/**
 * Marked
 */

function marked(src, opt, callback) {
  if (callback || typeof opt === 'function') {
    if (!callback) {
      callback = opt;
      opt = null;
    }

    opt = merge({}, marked.defaults, opt || {});

    var highlight = opt.highlight
      , tokens
      , pending
      , i = 0;

    try {
      tokens = Lexer.lex(src, opt)
    } catch (e) {
      return callback(e);
    }

    pending = tokens.length;

    var done = function() {
      var out, err;

      try {
        out = Parser.parse(tokens, opt);
      } catch (e) {
        err = e;
      }

      opt.highlight = highlight;

      return err
        ? callback(err)
        : callback(null, out);
    };

    if (!highlight || highlight.length < 3) {
      return done();
    }

    delete opt.highlight;

    if (!pending) return done();

    for (; i < tokens.length; i++) {
      (function(token) {
        if (token.type !== 'code') {
          return --pending || done();
        }
        return highlight(token.text, token.lang, function(err, code) {
          if (code == null || code === token.text) {
            return --pending || done();
          }
          token.text = code;
          token.escaped = true;
          --pending || done();
        });
      })(tokens[i]);
    }

    return;
  }
  try {
    if (opt) opt = merge({}, marked.defaults, opt);
    return Parser.parse(Lexer.lex(src, opt), opt);
  } catch (e) {
    e.message += '\nPlease report this to https://github.com/chjj/marked.';
    if ((opt || marked.defaults).silent) {
      return '<p>An error occured:</p><pre>'
        + escape(e.message + '', true)
        + '</pre>';
    }
    throw e;
  }
}

/**
 * Options
 */

marked.options =
marked.setOptions = function(opt) {
  merge(marked.defaults, opt);
  return marked;
};

marked.defaults = {
  gfm: true,
  tables: true,
  breaks: false,
  pedantic: false,
  sanitize: false,
  smartLists: false,
  silent: false,
  highlight: null,
  langPrefix: 'lang-',
  smartypants: false,
  headerPrefix: '',
  renderer: new Renderer,
  xhtml: false
};

/**
 * Expose
 */

marked.Parser = Parser;
marked.parser = Parser.parse;

marked.Renderer = Renderer;

marked.Lexer = Lexer;
marked.lexer = Lexer.lex;

marked.InlineLexer = InlineLexer;
marked.inlineLexer = InlineLexer.output;

marked.parse = marked;

if (typeof exports === 'object') {
  module.exports = marked;
} else if (typeof define === 'function' && define.amd) {
  define(function() { return marked; });
} else {
  this.marked = marked;
}

}).call(function() {
  return this || (typeof window !== 'undefined' ? window : global);
}());

// Released under MIT license
// Copyright (c) 2009-2010 Dominic Baggott
// Copyright (c) 2009-2010 Ash Berlin
// Copyright (c) 2011 Christoph Dorn <christoph@christophdorn.com> (http://www.christophdorn.com)

(function( expose ) {

/**
 *  class Markdown
 *
 *  Markdown processing in Javascript done right. We have very particular views
 *  on what constitutes 'right' which include:
 *
 *  - produces well-formed HTML (this means that em and strong nesting is
 *    important)
 *
 *  - has an intermediate representation to allow processing of parsed data (We
 *    in fact have two, both as [JsonML]: a markdown tree and an HTML tree).
 *
 *  - is easily extensible to add new dialects without having to rewrite the
 *    entire parsing mechanics
 *
 *  - has a good test suite
 *
 *  This implementation fulfills all of these (except that the test suite could
 *  do with expanding to automatically run all the fixtures from other Markdown
 *  implementations.)
 *
 *  ##### Intermediate Representation
 *
 *  *TODO* Talk about this :) Its JsonML, but document the node names we use.
 *
 *  [JsonML]: http://jsonml.org/ "JSON Markup Language"
 **/
var Markdown = expose.Markdown = function Markdown(dialect) {
  switch (typeof dialect) {
    case "undefined":
      this.dialect = Markdown.dialects.Gruber;
      break;
    case "object":
      this.dialect = dialect;
      break;
    default:
      if (dialect in Markdown.dialects) {
        this.dialect = Markdown.dialects[dialect];
      }
      else {
        throw new Error("Unknown Markdown dialect '" + String(dialect) + "'");
      }
      break;
  }
  this.em_state = [];
  this.strong_state = [];
  this.debug_indent = "";
};

/**
 *  parse( markdown, [dialect] ) -> JsonML
 *  - markdown (String): markdown string to parse
 *  - dialect (String | Dialect): the dialect to use, defaults to gruber
 *
 *  Parse `markdown` and return a markdown document as a Markdown.JsonML tree.
 **/
expose.parse = function( source, dialect ) {
  // dialect will default if undefined
  var md = new Markdown( dialect );
  return md.toTree( source );
};

/**
 *  toHTML( markdown, [dialect]  ) -> String
 *  toHTML( md_tree ) -> String
 *  - markdown (String): markdown string to parse
 *  - md_tree (Markdown.JsonML): parsed markdown tree
 *
 *  Take markdown (either as a string or as a JsonML tree) and run it through
 *  [[toHTMLTree]] then turn it into a well-formated HTML fragment.
 **/
expose.toHTML = function toHTML( source , dialect , options ) {
  var input = expose.toHTMLTree( source , dialect , options );

  return expose.renderJsonML( input );
};

/**
 *  toHTMLTree( markdown, [dialect] ) -> JsonML
 *  toHTMLTree( md_tree ) -> JsonML
 *  - markdown (String): markdown string to parse
 *  - dialect (String | Dialect): the dialect to use, defaults to gruber
 *  - md_tree (Markdown.JsonML): parsed markdown tree
 *
 *  Turn markdown into HTML, represented as a JsonML tree. If a string is given
 *  to this function, it is first parsed into a markdown tree by calling
 *  [[parse]].
 **/
expose.toHTMLTree = function toHTMLTree( input, dialect , options ) {
  // convert string input to an MD tree
  if ( typeof input ==="string" ) input = this.parse( input, dialect );

  // Now convert the MD tree to an HTML tree

  // remove references from the tree
  var attrs = extract_attr( input ),
      refs = {};

  if ( attrs && attrs.references ) {
    refs = attrs.references;
  }

  var html = convert_tree_to_html( input, refs , options );
  merge_text_nodes( html );
  return html;
};

// For Spidermonkey based engines
function mk_block_toSource() {
  return "Markdown.mk_block( " +
          uneval(this.toString()) +
          ", " +
          uneval(this.trailing) +
          ", " +
          uneval(this.lineNumber) +
          " )";
}

// node
function mk_block_inspect() {
  var util = require('util');
  return "Markdown.mk_block( " +
          util.inspect(this.toString()) +
          ", " +
          util.inspect(this.trailing) +
          ", " +
          util.inspect(this.lineNumber) +
          " )";

}

var mk_block = Markdown.mk_block = function(block, trail, line) {
  // Be helpful for default case in tests.
  if ( arguments.length == 1 ) trail = "\n\n";

  var s = new String(block);
  s.trailing = trail;
  // To make it clear its not just a string
  s.inspect = mk_block_inspect;
  s.toSource = mk_block_toSource;

  if (line != undefined)
    s.lineNumber = line;

  return s;
};

function count_lines( str ) {
  var n = 0, i = -1;
  while ( ( i = str.indexOf('\n', i+1) ) !== -1) n++;
  return n;
}

// Internal - split source into rough blocks
Markdown.prototype.split_blocks = function splitBlocks( input, startLine ) {
  // [\s\S] matches _anything_ (newline or space)
  var re = /([\s\S]+?)($|\n(?:\s*\n|$)+)/g,
      blocks = [],
      m;

  var line_no = 1;

  if ( ( m = /^(\s*\n)/.exec(input) ) != null ) {
    // skip (but count) leading blank lines
    line_no += count_lines( m[0] );
    re.lastIndex = m[0].length;
  }

  while ( ( m = re.exec(input) ) !== null ) {
    blocks.push( mk_block( m[1], m[2], line_no ) );
    line_no += count_lines( m[0] );
  }

  return blocks;
};

/**
 *  Markdown#processBlock( block, next ) -> undefined | [ JsonML, ... ]
 *  - block (String): the block to process
 *  - next (Array): the following blocks
 *
 * Process `block` and return an array of JsonML nodes representing `block`.
 *
 * It does this by asking each block level function in the dialect to process
 * the block until one can. Succesful handling is indicated by returning an
 * array (with zero or more JsonML nodes), failure by a false value.
 *
 * Blocks handlers are responsible for calling [[Markdown#processInline]]
 * themselves as appropriate.
 *
 * If the blocks were split incorrectly or adjacent blocks need collapsing you
 * can adjust `next` in place using shift/splice etc.
 *
 * If any of this default behaviour is not right for the dialect, you can
 * define a `__call__` method on the dialect that will get invoked to handle
 * the block processing.
 */
Markdown.prototype.processBlock = function processBlock( block, next ) {
  var cbs = this.dialect.block,
      ord = cbs.__order__;

  if ( "__call__" in cbs ) {
    return cbs.__call__.call(this, block, next);
  }

  for ( var i = 0; i < ord.length; i++ ) {
    //D:this.debug( "Testing", ord[i] );
    var res = cbs[ ord[i] ].call( this, block, next );
    if ( res ) {
      //D:this.debug("  matched");
      if ( !isArray(res) || ( res.length > 0 && !( isArray(res[0]) ) ) )
        this.debug(ord[i], "didn't return a proper array");
      //D:this.debug( "" );
      return res;
    }
  }

  // Uhoh! no match! Should we throw an error?
  return [];
};

Markdown.prototype.processInline = function processInline( block ) {
  return this.dialect.inline.__call__.call( this, String( block ) );
};

/**
 *  Markdown#toTree( source ) -> JsonML
 *  - source (String): markdown source to parse
 *
 *  Parse `source` into a JsonML tree representing the markdown document.
 **/
// custom_tree means set this.tree to `custom_tree` and restore old value on return
Markdown.prototype.toTree = function toTree( source, custom_root ) {
  var blocks = source instanceof Array ? source : this.split_blocks( source );

  // Make tree a member variable so its easier to mess with in extensions
  var old_tree = this.tree;
  try {
    this.tree = custom_root || this.tree || [ "markdown" ];

    blocks:
    while ( blocks.length ) {
      var b = this.processBlock( blocks.shift(), blocks );

      // Reference blocks and the like won't return any content
      if ( !b.length ) continue blocks;

      this.tree.push.apply( this.tree, b );
    }
    return this.tree;
  }
  finally {
    if ( custom_root ) {
      this.tree = old_tree;
    }
  }
};

// Noop by default
Markdown.prototype.debug = function () {
  var args = Array.prototype.slice.call( arguments);
  args.unshift(this.debug_indent);
  if (typeof print !== "undefined")
      print.apply( print, args );
  if (typeof console !== "undefined" && typeof console.log !== "undefined")
      console.log.apply( null, args );
}

Markdown.prototype.loop_re_over_block = function( re, block, cb ) {
  // Dont use /g regexps with this
  var m,
      b = block.valueOf();

  while ( b.length && (m = re.exec(b) ) != null) {
    b = b.substr( m[0].length );
    cb.call(this, m);
  }
  return b;
};

/**
 * Markdown.dialects
 *
 * Namespace of built-in dialects.
 **/
Markdown.dialects = {};

/**
 * Markdown.dialects.Gruber
 *
 * The default dialect that follows the rules set out by John Gruber's
 * markdown.pl as closely as possible. Well actually we follow the behaviour of
 * that script which in some places is not exactly what the syntax web page
 * says.
 **/
Markdown.dialects.Gruber = {
  block: {
    atxHeader: function atxHeader( block, next ) {
      var m = block.match( /^(#{1,6})\s*(.*?)\s*#*\s*(?:\n|$)/ );

      if ( !m ) return undefined;

      var header = [ "header", { level: m[ 1 ].length } ];
      Array.prototype.push.apply(header, this.processInline(m[ 2 ]));

      if ( m[0].length < block.length )
        next.unshift( mk_block( block.substr( m[0].length ), block.trailing, block.lineNumber + 2 ) );

      return [ header ];
    },

    setextHeader: function setextHeader( block, next ) {
      var m = block.match( /^(.*)\n([-=])\2\2+(?:\n|$)/ );

      if ( !m ) return undefined;

      var level = ( m[ 2 ] === "=" ) ? 1 : 2;
      var header = [ "header", { level : level }, m[ 1 ] ];

      if ( m[0].length < block.length )
        next.unshift( mk_block( block.substr( m[0].length ), block.trailing, block.lineNumber + 2 ) );

      return [ header ];
    },

    code: function code( block, next ) {
      // |    Foo
      // |bar
      // should be a code block followed by a paragraph. Fun
      //
      // There might also be adjacent code block to merge.

      var ret = [],
          re = /^(?: {0,3}\t| {4})(.*)\n?/,
          lines;

      // 4 spaces + content
      if ( !block.match( re ) ) return undefined;

      block_search:
      do {
        // Now pull out the rest of the lines
        var b = this.loop_re_over_block(
                  re, block.valueOf(), function( m ) { ret.push( m[1] ); } );

        if (b.length) {
          // Case alluded to in first comment. push it back on as a new block
          next.unshift( mk_block(b, block.trailing) );
          break block_search;
        }
        else if (next.length) {
          // Check the next block - it might be code too
          if ( !next[0].match( re ) ) break block_search;

          // Pull how how many blanks lines follow - minus two to account for .join
          ret.push ( block.trailing.replace(/[^\n]/g, '').substring(2) );

          block = next.shift();
        }
        else {
          break block_search;
        }
      } while (true);

      return [ [ "code_block", ret.join("\n") ] ];
    },

    horizRule: function horizRule( block, next ) {
      // this needs to find any hr in the block to handle abutting blocks
      var m = block.match( /^(?:([\s\S]*?)\n)?[ \t]*([-_*])(?:[ \t]*\2){2,}[ \t]*(?:\n([\s\S]*))?$/ );

      if ( !m ) {
        return undefined;
      }

      var jsonml = [ [ "hr" ] ];

      // if there's a leading abutting block, process it
      if ( m[ 1 ] ) {
        jsonml.unshift.apply( jsonml, this.processBlock( m[ 1 ], [] ) );
      }

      // if there's a trailing abutting block, stick it into next
      if ( m[ 3 ] ) {
        next.unshift( mk_block( m[ 3 ] ) );
      }

      return jsonml;
    },

    // There are two types of lists. Tight and loose. Tight lists have no whitespace
    // between the items (and result in text just in the <li>) and loose lists,
    // which have an empty line between list items, resulting in (one or more)
    // paragraphs inside the <li>.
    //
    // There are all sorts weird edge cases about the original markdown.pl's
    // handling of lists:
    //
    // * Nested lists are supposed to be indented by four chars per level. But
    //   if they aren't, you can get a nested list by indenting by less than
    //   four so long as the indent doesn't match an indent of an existing list
    //   item in the 'nest stack'.
    //
    // * The type of the list (bullet or number) is controlled just by the
    //    first item at the indent. Subsequent changes are ignored unless they
    //    are for nested lists
    //
    lists: (function( ) {
      // Use a closure to hide a few variables.
      var any_list = "[*+-]|\\d+\\.",
          bullet_list = /[*+-]/,
          number_list = /\d+\./,
          // Capture leading indent as it matters for determining nested lists.
          is_list_re = new RegExp( "^( {0,3})(" + any_list + ")[ \t]+" ),
          indent_re = "(?: {0,3}\\t| {4})";

      // TODO: Cache this regexp for certain depths.
      // Create a regexp suitable for matching an li for a given stack depth
      function regex_for_depth( depth ) {

        return new RegExp(
          // m[1] = indent, m[2] = list_type
          "(?:^(" + indent_re + "{0," + depth + "} {0,3})(" + any_list + ")\\s+)|" +
          // m[3] = cont
          "(^" + indent_re + "{0," + (depth-1) + "}[ ]{0,4})"
        );
      }
      function expand_tab( input ) {
        return input.replace( / {0,3}\t/g, "    " );
      }

      // Add inline content `inline` to `li`. inline comes from processInline
      // so is an array of content
      function add(li, loose, inline, nl) {
        if (loose) {
          li.push( [ "para" ].concat(inline) );
          return;
        }
        // Hmmm, should this be any block level element or just paras?
        var add_to = li[li.length -1] instanceof Array && li[li.length - 1][0] == "para"
                   ? li[li.length -1]
                   : li;

        // If there is already some content in this list, add the new line in
        if (nl && li.length > 1) inline.unshift(nl);

        for (var i=0; i < inline.length; i++) {
          var what = inline[i],
              is_str = typeof what == "string";
          if (is_str && add_to.length > 1 && typeof add_to[add_to.length-1] == "string" ) {
            add_to[ add_to.length-1 ] += what;
          }
          else {
            add_to.push( what );
          }
        }
      }

      // contained means have an indent greater than the current one. On
      // *every* line in the block
      function get_contained_blocks( depth, blocks ) {

        var re = new RegExp( "^(" + indent_re + "{" + depth + "}.*?\\n?)*$" ),
            replace = new RegExp("^" + indent_re + "{" + depth + "}", "gm"),
            ret = [];

        while ( blocks.length > 0 ) {
          if ( re.exec( blocks[0] ) ) {
            var b = blocks.shift(),
                // Now remove that indent
                x = b.replace( replace, "");

            ret.push( mk_block( x, b.trailing, b.lineNumber ) );
          }
          break;
        }
        return ret;
      }

      // passed to stack.forEach to turn list items up the stack into paras
      function paragraphify(s, i, stack) {
        var list = s.list;
        var last_li = list[list.length-1];

        if (last_li[1] instanceof Array && last_li[1][0] == "para") {
          return;
        }
        if (i+1 == stack.length) {
          // Last stack frame
          // Keep the same array, but replace the contents
          last_li.push( ["para"].concat( last_li.splice(1) ) );
        }
        else {
          var sublist = last_li.pop();
          last_li.push( ["para"].concat( last_li.splice(1) ), sublist );
        }
      }

      // The matcher function
      return function( block, next ) {
        var m = block.match( is_list_re );
        if ( !m ) return undefined;

        function make_list( m ) {
          var list = bullet_list.exec( m[2] )
                   ? ["bulletlist"]
                   : ["numberlist"];

          stack.push( { list: list, indent: m[1] } );
          return list;
        }


        var stack = [], // Stack of lists for nesting.
            list = make_list( m ),
            last_li,
            loose = false,
            ret = [ stack[0].list ],
            i;

        // Loop to search over block looking for inner block elements and loose lists
        loose_search:
        while( true ) {
          // Split into lines preserving new lines at end of line
          var lines = block.split( /(?=\n)/ );

          // We have to grab all lines for a li and call processInline on them
          // once as there are some inline things that can span lines.
          var li_accumulate = "";

          // Loop over the lines in this block looking for tight lists.
          tight_search:
          for (var line_no=0; line_no < lines.length; line_no++) {
            var nl = "",
                l = lines[line_no].replace(/^\n/, function(n) { nl = n; return ""; });

            // TODO: really should cache this
            var line_re = regex_for_depth( stack.length );

            m = l.match( line_re );
            //print( "line:", uneval(l), "\nline match:", uneval(m) );

            // We have a list item
            if ( m[1] !== undefined ) {
              // Process the previous list item, if any
              if ( li_accumulate.length ) {
                add( last_li, loose, this.processInline( li_accumulate ), nl );
                // Loose mode will have been dealt with. Reset it
                loose = false;
                li_accumulate = "";
              }

              m[1] = expand_tab( m[1] );
              var wanted_depth = Math.floor(m[1].length/4)+1;
              //print( "want:", wanted_depth, "stack:", stack.length);
              if ( wanted_depth > stack.length ) {
                // Deep enough for a nested list outright
                //print ( "new nested list" );
                list = make_list( m );
                last_li.push( list );
                last_li = list[1] = [ "listitem" ];
              }
              else {
                // We aren't deep enough to be strictly a new level. This is
                // where Md.pl goes nuts. If the indent matches a level in the
                // stack, put it there, else put it one deeper then the
                // wanted_depth deserves.
                var found = false;
                for (i = 0; i < stack.length; i++) {
                  if ( stack[ i ].indent != m[1] ) continue;
                  list = stack[ i ].list;
                  stack.splice( i+1 );
                  found = true;
                  break;
                }

                if (!found) {
                  //print("not found. l:", uneval(l));
                  wanted_depth++;
                  if (wanted_depth <= stack.length) {
                    stack.splice(wanted_depth);
                    //print("Desired depth now", wanted_depth, "stack:", stack.length);
                    list = stack[wanted_depth-1].list;
                    //print("list:", uneval(list) );
                  }
                  else {
                    //print ("made new stack for messy indent");
                    list = make_list(m);
                    last_li.push(list);
                  }
                }

                //print( uneval(list), "last", list === stack[stack.length-1].list );
                last_li = [ "listitem" ];
                list.push(last_li);
              } // end depth of shenegains
              nl = "";
            }

            // Add content
            if (l.length > m[0].length) {
              li_accumulate += nl + l.substr( m[0].length );
            }
          } // tight_search

          if ( li_accumulate.length ) {
            add( last_li, loose, this.processInline( li_accumulate ), nl );
            // Loose mode will have been dealt with. Reset it
            loose = false;
            li_accumulate = "";
          }

          // Look at the next block - we might have a loose list. Or an extra
          // paragraph for the current li
          var contained = get_contained_blocks( stack.length, next );

          // Deal with code blocks or properly nested lists
          if (contained.length > 0) {
            // Make sure all listitems up the stack are paragraphs
            forEach( stack, paragraphify, this);

            last_li.push.apply( last_li, this.toTree( contained, [] ) );
          }

          var next_block = next[0] && next[0].valueOf() || "";

          if ( next_block.match(is_list_re) || next_block.match( /^ / ) ) {
            block = next.shift();

            // Check for an HR following a list: features/lists/hr_abutting
            var hr = this.dialect.block.horizRule( block, next );

            if (hr) {
              ret.push.apply(ret, hr);
              break;
            }

            // Make sure all listitems up the stack are paragraphs
            forEach( stack, paragraphify, this);

            loose = true;
            continue loose_search;
          }
          break;
        } // loose_search

        return ret;
      };
    })(),

    blockquote: function blockquote( block, next ) {
      if ( !block.match( /^>/m ) )
        return undefined;

      var jsonml = [];

      // separate out the leading abutting block, if any
      if ( block[ 0 ] != ">" ) {
        var lines = block.split( /\n/ ),
            prev = [];

        // keep shifting lines until you find a crotchet
        while ( lines.length && lines[ 0 ][ 0 ] != ">" ) {
            prev.push( lines.shift() );
        }

        // reassemble!
        block = lines.join( "\n" );
        jsonml.push.apply( jsonml, this.processBlock( prev.join( "\n" ), [] ) );
      }

      // if the next block is also a blockquote merge it in
      while ( next.length && next[ 0 ][ 0 ] == ">" ) {
        var b = next.shift();
        block = new String(block + block.trailing + b);
        block.trailing = b.trailing;
      }

      // Strip off the leading "> " and re-process as a block.
      var input = block.replace( /^> ?/gm, '' ),
          old_tree = this.tree;
      jsonml.push( this.toTree( input, [ "blockquote" ] ) );

      return jsonml;
    },

    referenceDefn: function referenceDefn( block, next) {
      var re = /^\s*\[(.*?)\]:\s*(\S+)(?:\s+(?:(['"])(.*?)\3|\((.*?)\)))?\n?/;
      // interesting matches are [ , ref_id, url, , title, title ]

      if ( !block.match(re) )
        return undefined;

      // make an attribute node if it doesn't exist
      if ( !extract_attr( this.tree ) ) {
        this.tree.splice( 1, 0, {} );
      }

      var attrs = extract_attr( this.tree );

      // make a references hash if it doesn't exist
      if ( attrs.references === undefined ) {
        attrs.references = {};
      }

      var b = this.loop_re_over_block(re, block, function( m ) {

        if ( m[2] && m[2][0] == '<' && m[2][m[2].length-1] == '>' )
          m[2] = m[2].substring( 1, m[2].length - 1 );

        var ref = attrs.references[ m[1].toLowerCase() ] = {
          href: m[2]
        };

        if (m[4] !== undefined)
          ref.title = m[4];
        else if (m[5] !== undefined)
          ref.title = m[5];

      } );

      if (b.length)
        next.unshift( mk_block( b, block.trailing ) );

      return [];
    },

    para: function para( block, next ) {
      // everything's a para!
      return [ ["para"].concat( this.processInline( block ) ) ];
    }
  }
};

Markdown.dialects.Gruber.inline = {

    __oneElement__: function oneElement( text, patterns_or_re, previous_nodes ) {
      var m,
          res,
          lastIndex = 0;

      patterns_or_re = patterns_or_re || this.dialect.inline.__patterns__;
      var re = new RegExp( "([\\s\\S]*?)(" + (patterns_or_re.source || patterns_or_re) + ")" );

      m = re.exec( text );
      if (!m) {
        // Just boring text
        return [ text.length, text ];
      }
      else if ( m[1] ) {
        // Some un-interesting text matched. Return that first
        return [ m[1].length, m[1] ];
      }

      var res;
      if ( m[2] in this.dialect.inline ) {
        res = this.dialect.inline[ m[2] ].call(
                  this,
                  text.substr( m.index ), m, previous_nodes || [] );
      }
      // Default for now to make dev easier. just slurp special and output it.
      res = res || [ m[2].length, m[2] ];
      return res;
    },

    __call__: function inline( text, patterns ) {

      var out = [],
          res;

      function add(x) {
        //D:self.debug("  adding output", uneval(x));
        if (typeof x == "string" && typeof out[out.length-1] == "string")
          out[ out.length-1 ] += x;
        else
          out.push(x);
      }

      while ( text.length > 0 ) {
        res = this.dialect.inline.__oneElement__.call(this, text, patterns, out );
        text = text.substr( res.shift() );
        forEach(res, add )
      }

      return out;
    },

    // These characters are intersting elsewhere, so have rules for them so that
    // chunks of plain text blocks don't include them
    "]": function () {},
    "}": function () {},

    "\\": function escaped( text ) {
      // [ length of input processed, node/children to add... ]
      // Only esacape: \ ` * _ { } [ ] ( ) # * + - . !
      if ( text.match( /^\\[\\`\*_{}\[\]()#\+.!\-]/ ) )
        return [ 2, text[1] ];
      else
        // Not an esacpe
        return [ 1, "\\" ];
    },

    "![": function image( text ) {

      // Unlike images, alt text is plain text only. no other elements are
      // allowed in there

      // ![Alt text](/path/to/img.jpg "Optional title")
      //      1          2            3       4         <--- captures
      var m = text.match( /^!\[(.*?)\][ \t]*\([ \t]*(\S*)(?:[ \t]+(["'])(.*?)\3)?[ \t]*\)/ );

      if ( m ) {
        if ( m[2] && m[2][0] == '<' && m[2][m[2].length-1] == '>' )
          m[2] = m[2].substring( 1, m[2].length - 1 );

        m[2] = this.dialect.inline.__call__.call( this, m[2], /\\/ )[0];

        var attrs = { alt: m[1], href: m[2] || "" };
        if ( m[4] !== undefined)
          attrs.title = m[4];

        return [ m[0].length, [ "img", attrs ] ];
      }

      // ![Alt text][id]
      m = text.match( /^!\[(.*?)\][ \t]*\[(.*?)\]/ );

      if ( m ) {
        // We can't check if the reference is known here as it likely wont be
        // found till after. Check it in md tree->hmtl tree conversion
        return [ m[0].length, [ "img_ref", { alt: m[1], ref: m[2].toLowerCase(), original: m[0] } ] ];
      }

      // Just consume the '!['
      return [ 2, "![" ];
    },

    "[": function link( text ) {

      var orig = String(text);
      // Inline content is possible inside `link text`
      var res = Markdown.DialectHelpers.inline_until_char.call( this, text.substr(1), ']' );

      // No closing ']' found. Just consume the [
      if ( !res ) return [ 1, '[' ];

      var consumed = 1 + res[ 0 ],
          children = res[ 1 ],
          link,
          attrs;

      // At this point the first [...] has been parsed. See what follows to find
      // out which kind of link we are (reference or direct url)
      text = text.substr( consumed );

      // [link text](/path/to/img.jpg "Optional title")
      //                 1            2       3         <--- captures
      // This will capture up to the last paren in the block. We then pull
      // back based on if there a matching ones in the url
      //    ([here](/url/(test))
      // The parens have to be balanced
      var m = text.match( /^\s*\([ \t]*(\S+)(?:[ \t]+(["'])(.*?)\2)?[ \t]*\)/ );
      if ( m ) {
        var url = m[1];
        consumed += m[0].length;

        if ( url && url[0] == '<' && url[url.length-1] == '>' )
          url = url.substring( 1, url.length - 1 );

        // If there is a title we don't have to worry about parens in the url
        if ( !m[3] ) {
          var open_parens = 1; // One open that isn't in the capture
          for (var len = 0; len < url.length; len++) {
            switch ( url[len] ) {
            case '(':
              open_parens++;
              break;
            case ')':
              if ( --open_parens == 0) {
                consumed -= url.length - len;
                url = url.substring(0, len);
              }
              break;
            }
          }
        }

        // Process escapes only
        url = this.dialect.inline.__call__.call( this, url, /\\/ )[0];

        attrs = { href: url || "" };
        if ( m[3] !== undefined)
          attrs.title = m[3];

        link = [ "link", attrs ].concat( children );
        return [ consumed, link ];
      }

      // [Alt text][id]
      // [Alt text] [id]
      m = text.match( /^\s*\[(.*?)\]/ );

      if ( m ) {

        consumed += m[ 0 ].length;

        // [links][] uses links as its reference
        attrs = { ref: ( m[ 1 ] || String(children) ).toLowerCase(),  original: orig.substr( 0, consumed ) };

        link = [ "link_ref", attrs ].concat( children );

        // We can't check if the reference is known here as it likely wont be
        // found till after. Check it in md tree->hmtl tree conversion.
        // Store the original so that conversion can revert if the ref isn't found.
        return [ consumed, link ];
      }

      // [id]
      // Only if id is plain (no formatting.)
      if ( children.length == 1 && typeof children[0] == "string" ) {

        attrs = { ref: children[0].toLowerCase(),  original: orig.substr( 0, consumed ) };
        link = [ "link_ref", attrs, children[0] ];
        return [ consumed, link ];
      }

      // Just consume the '['
      return [ 1, "[" ];
    },


    "<": function autoLink( text ) {
      var m;

      if ( ( m = text.match( /^<(?:((https?|ftp|mailto):[^>]+)|(.*?@.*?\.[a-zA-Z]+))>/ ) ) != null ) {
        if ( m[3] ) {
          return [ m[0].length, [ "link", { href: "mailto:" + m[3] }, m[3] ] ];

        }
        else if ( m[2] == "mailto" ) {
          return [ m[0].length, [ "link", { href: m[1] }, m[1].substr("mailto:".length ) ] ];
        }
        else
          return [ m[0].length, [ "link", { href: m[1] }, m[1] ] ];
      }

      return [ 1, "<" ];
    },

    "`": function inlineCode( text ) {
      // Inline code block. as many backticks as you like to start it
      // Always skip over the opening ticks.
      var m = text.match( /(`+)(([\s\S]*?)\1)/ );

      if ( m && m[2] )
        return [ m[1].length + m[2].length, [ "inlinecode", m[3] ] ];
      else {
        // TODO: No matching end code found - warn!
        return [ 1, "`" ];
      }
    },

    "  \n": function lineBreak( text ) {
      return [ 3, [ "linebreak" ] ];
    }

};

// Meta Helper/generator method for em and strong handling
function strong_em( tag, md ) {

  var state_slot = tag + "_state",
      other_slot = tag == "strong" ? "em_state" : "strong_state";

  function CloseTag(len) {
    this.len_after = len;
    this.name = "close_" + md;
  }

  return function ( text, orig_match ) {

    if (this[state_slot][0] == md) {
      // Most recent em is of this type
      //D:this.debug("closing", md);
      this[state_slot].shift();

      // "Consume" everything to go back to the recrusion in the else-block below
      return[ text.length, new CloseTag(text.length-md.length) ];
    }
    else {
      // Store a clone of the em/strong states
      var other = this[other_slot].slice(),
          state = this[state_slot].slice();

      this[state_slot].unshift(md);

      //D:this.debug_indent += "  ";

      // Recurse
      var res = this.processInline( text.substr( md.length ) );
      //D:this.debug_indent = this.debug_indent.substr(2);

      var last = res[res.length - 1];

      //D:this.debug("processInline from", tag + ": ", uneval( res ) );

      var check = this[state_slot].shift();
      if (last instanceof CloseTag) {
        res.pop();
        // We matched! Huzzah.
        var consumed = text.length - last.len_after;
        return [ consumed, [ tag ].concat(res) ];
      }
      else {
        // Restore the state of the other kind. We might have mistakenly closed it.
        this[other_slot] = other;
        this[state_slot] = state;

        // We can't reuse the processed result as it could have wrong parsing contexts in it.
        return [ md.length, md ];
      }
    }
  }; // End returned function
}

Markdown.dialects.Gruber.inline["**"] = strong_em("strong", "**");
Markdown.dialects.Gruber.inline["__"] = strong_em("strong", "__");
Markdown.dialects.Gruber.inline["*"]  = strong_em("em", "*");
Markdown.dialects.Gruber.inline["_"]  = strong_em("em", "_");


// Build default order from insertion order.
Markdown.buildBlockOrder = function(d) {
  var ord = [];
  for ( var i in d ) {
    if ( i == "__order__" || i == "__call__" ) continue;
    ord.push( i );
  }
  d.__order__ = ord;
};

// Build patterns for inline matcher
Markdown.buildInlinePatterns = function(d) {
  var patterns = [];

  for ( var i in d ) {
    // __foo__ is reserved and not a pattern
    if ( i.match( /^__.*__$/) ) continue;
    var l = i.replace( /([\\.*+?|()\[\]{}])/g, "\\$1" )
             .replace( /\n/, "\\n" );
    patterns.push( i.length == 1 ? l : "(?:" + l + ")" );
  }

  patterns = patterns.join("|");
  d.__patterns__ = patterns;
  //print("patterns:", uneval( patterns ) );

  var fn = d.__call__;
  d.__call__ = function(text, pattern) {
    if (pattern != undefined) {
      return fn.call(this, text, pattern);
    }
    else
    {
      return fn.call(this, text, patterns);
    }
  };
};

Markdown.DialectHelpers = {};
Markdown.DialectHelpers.inline_until_char = function( text, want ) {
  var consumed = 0,
      nodes = [];

  while ( true ) {
    if ( text[ consumed ] == want ) {
      // Found the character we were looking for
      consumed++;
      return [ consumed, nodes ];
    }

    if ( consumed >= text.length ) {
      // No closing char found. Abort.
      return null;
    }

    var res = this.dialect.inline.__oneElement__.call(this, text.substr( consumed ) );
    consumed += res[ 0 ];
    // Add any returned nodes.
    nodes.push.apply( nodes, res.slice( 1 ) );
  }
}

// Helper function to make sub-classing a dialect easier
Markdown.subclassDialect = function( d ) {
  function Block() {}
  Block.prototype = d.block;
  function Inline() {}
  Inline.prototype = d.inline;

  return { block: new Block(), inline: new Inline() };
};

Markdown.buildBlockOrder ( Markdown.dialects.Gruber.block );
Markdown.buildInlinePatterns( Markdown.dialects.Gruber.inline );

Markdown.dialects.Maruku = Markdown.subclassDialect( Markdown.dialects.Gruber );

Markdown.dialects.Maruku.processMetaHash = function processMetaHash( meta_string ) {
  var meta = split_meta_hash( meta_string ),
      attr = {};

  for ( var i = 0; i < meta.length; ++i ) {
    // id: #foo
    if ( /^#/.test( meta[ i ] ) ) {
      attr.id = meta[ i ].substring( 1 );
    }
    // class: .foo
    else if ( /^\./.test( meta[ i ] ) ) {
      // if class already exists, append the new one
      if ( attr['class'] ) {
        attr['class'] = attr['class'] + meta[ i ].replace( /./, " " );
      }
      else {
        attr['class'] = meta[ i ].substring( 1 );
      }
    }
    // attribute: foo=bar
    else if ( /\=/.test( meta[ i ] ) ) {
      var s = meta[ i ].split( /\=/ );
      attr[ s[ 0 ] ] = s[ 1 ];
    }
  }

  return attr;
}

function split_meta_hash( meta_string ) {
  var meta = meta_string.split( "" ),
      parts = [ "" ],
      in_quotes = false;

  while ( meta.length ) {
    var letter = meta.shift();
    switch ( letter ) {
      case " " :
        // if we're in a quoted section, keep it
        if ( in_quotes ) {
          parts[ parts.length - 1 ] += letter;
        }
        // otherwise make a new part
        else {
          parts.push( "" );
        }
        break;
      case "'" :
      case '"' :
        // reverse the quotes and move straight on
        in_quotes = !in_quotes;
        break;
      case "\\" :
        // shift off the next letter to be used straight away.
        // it was escaped so we'll keep it whatever it is
        letter = meta.shift();
      default :
        parts[ parts.length - 1 ] += letter;
        break;
    }
  }

  return parts;
}

Markdown.dialects.Maruku.block.document_meta = function document_meta( block, next ) {
  // we're only interested in the first block
  if ( block.lineNumber > 1 ) return undefined;

  // document_meta blocks consist of one or more lines of `Key: Value\n`
  if ( ! block.match( /^(?:\w+:.*\n)*\w+:.*$/ ) ) return undefined;

  // make an attribute node if it doesn't exist
  if ( !extract_attr( this.tree ) ) {
    this.tree.splice( 1, 0, {} );
  }

  var pairs = block.split( /\n/ );
  for ( p in pairs ) {
    var m = pairs[ p ].match( /(\w+):\s*(.*)$/ ),
        key = m[ 1 ].toLowerCase(),
        value = m[ 2 ];

    this.tree[ 1 ][ key ] = value;
  }

  // document_meta produces no content!
  return [];
};

Markdown.dialects.Maruku.block.block_meta = function block_meta( block, next ) {
  // check if the last line of the block is an meta hash
  var m = block.match( /(^|\n) {0,3}\{:\s*((?:\\\}|[^\}])*)\s*\}$/ );
  if ( !m ) return undefined;

  // process the meta hash
  var attr = this.dialect.processMetaHash( m[ 2 ] );

  var hash;

  // if we matched ^ then we need to apply meta to the previous block
  if ( m[ 1 ] === "" ) {
    var node = this.tree[ this.tree.length - 1 ];
    hash = extract_attr( node );

    // if the node is a string (rather than JsonML), bail
    if ( typeof node === "string" ) return undefined;

    // create the attribute hash if it doesn't exist
    if ( !hash ) {
      hash = {};
      node.splice( 1, 0, hash );
    }

    // add the attributes in
    for ( a in attr ) {
      hash[ a ] = attr[ a ];
    }

    // return nothing so the meta hash is removed
    return [];
  }

  // pull the meta hash off the block and process what's left
  var b = block.replace( /\n.*$/, "" ),
      result = this.processBlock( b, [] );

  // get or make the attributes hash
  hash = extract_attr( result[ 0 ] );
  if ( !hash ) {
    hash = {};
    result[ 0 ].splice( 1, 0, hash );
  }

  // attach the attributes to the block
  for ( a in attr ) {
    hash[ a ] = attr[ a ];
  }

  return result;
};

Markdown.dialects.Maruku.block.definition_list = function definition_list( block, next ) {
  // one or more terms followed by one or more definitions, in a single block
  var tight = /^((?:[^\s:].*\n)+):\s+([\s\S]+)$/,
      list = [ "dl" ],
      i;

  // see if we're dealing with a tight or loose block
  if ( ( m = block.match( tight ) ) ) {
    // pull subsequent tight DL blocks out of `next`
    var blocks = [ block ];
    while ( next.length && tight.exec( next[ 0 ] ) ) {
      blocks.push( next.shift() );
    }

    for ( var b = 0; b < blocks.length; ++b ) {
      var m = blocks[ b ].match( tight ),
          terms = m[ 1 ].replace( /\n$/, "" ).split( /\n/ ),
          defns = m[ 2 ].split( /\n:\s+/ );

      // print( uneval( m ) );

      for ( i = 0; i < terms.length; ++i ) {
        list.push( [ "dt", terms[ i ] ] );
      }

      for ( i = 0; i < defns.length; ++i ) {
        // run inline processing over the definition
        list.push( [ "dd" ].concat( this.processInline( defns[ i ].replace( /(\n)\s+/, "$1" ) ) ) );
      }
    }
  }
  else {
    return undefined;
  }

  return [ list ];
};

Markdown.dialects.Maruku.inline[ "{:" ] = function inline_meta( text, matches, out ) {
  if ( !out.length ) {
    return [ 2, "{:" ];
  }

  // get the preceeding element
  var before = out[ out.length - 1 ];

  if ( typeof before === "string" ) {
    return [ 2, "{:" ];
  }

  // match a meta hash
  var m = text.match( /^\{:\s*((?:\\\}|[^\}])*)\s*\}/ );

  // no match, false alarm
  if ( !m ) {
    return [ 2, "{:" ];
  }

  // attach the attributes to the preceeding element
  var meta = this.dialect.processMetaHash( m[ 1 ] ),
      attr = extract_attr( before );

  if ( !attr ) {
    attr = {};
    before.splice( 1, 0, attr );
  }

  for ( var k in meta ) {
    attr[ k ] = meta[ k ];
  }

  // cut out the string and replace it with nothing
  return [ m[ 0 ].length, "" ];
};

Markdown.buildBlockOrder ( Markdown.dialects.Maruku.block );
Markdown.buildInlinePatterns( Markdown.dialects.Maruku.inline );

var isArray = Array.isArray || function(obj) {
  return Object.prototype.toString.call(obj) == '[object Array]';
};

var forEach;
// Don't mess with Array.prototype. Its not friendly
if ( Array.prototype.forEach ) {
  forEach = function( arr, cb, thisp ) {
    return arr.forEach( cb, thisp );
  };
}
else {
  forEach = function(arr, cb, thisp) {
    for (var i = 0; i < arr.length; i++) {
      cb.call(thisp || arr, arr[i], i, arr);
    }
  }
}

function extract_attr( jsonml ) {
  return isArray(jsonml)
      && jsonml.length > 1
      && typeof jsonml[ 1 ] === "object"
      && !( isArray(jsonml[ 1 ]) )
      ? jsonml[ 1 ]
      : undefined;
}



/**
 *  renderJsonML( jsonml[, options] ) -> String
 *  - jsonml (Array): JsonML array to render to XML
 *  - options (Object): options
 *
 *  Converts the given JsonML into well-formed XML.
 *
 *  The options currently understood are:
 *
 *  - root (Boolean): wether or not the root node should be included in the
 *    output, or just its children. The default `false` is to not include the
 *    root itself.
 */
expose.renderJsonML = function( jsonml, options ) {
  options = options || {};
  // include the root element in the rendered output?
  options.root = options.root || false;

  var content = [];

  if ( options.root ) {
    content.push( render_tree( jsonml ) );
  }
  else {
    jsonml.shift(); // get rid of the tag
    if ( jsonml.length && typeof jsonml[ 0 ] === "object" && !( jsonml[ 0 ] instanceof Array ) ) {
      jsonml.shift(); // get rid of the attributes
    }

    while ( jsonml.length ) {
      content.push( render_tree( jsonml.shift() ) );
    }
  }

  return content.join( "\n\n" );
};

function escapeHTML( text ) {
  return text.replace( /&/g, "&amp;" )
             .replace( /</g, "&lt;" )
             .replace( />/g, "&gt;" )
             .replace( /"/g, "&quot;" )
             .replace( /'/g, "&#39;" );
}

function render_tree( jsonml ) {
  // basic case
  if ( typeof jsonml === "string" ) {
    return escapeHTML( jsonml );
  }

  var tag = jsonml.shift(),
      attributes = {},
      content = [];

  if ( jsonml.length && typeof jsonml[ 0 ] === "object" && !( jsonml[ 0 ] instanceof Array ) ) {
    attributes = jsonml.shift();
  }

  while ( jsonml.length ) {
    content.push( arguments.callee( jsonml.shift() ) );
  }

  var tag_attrs = "";
  for ( var a in attributes ) {
    tag_attrs += " " + a + '="' + escapeHTML( attributes[ a ] ) + '"';
  }

  // be careful about adding whitespace here for inline elements
  if ( tag == "img" || tag == "br" || tag == "hr" ) {
    return "<"+ tag + tag_attrs + "/>";
  }
  else {
    return "<"+ tag + tag_attrs + ">" + content.join( "" ) + "</" + tag + ">";
  }
}

function convert_tree_to_html( tree, references, options ) {
  var i;
  options = options || {};

  // shallow clone
  var jsonml = tree.slice( 0 );

  if (typeof options.preprocessTreeNode === "function") {
      jsonml = options.preprocessTreeNode(jsonml, references);
  }

  // Clone attributes if they exist
  var attrs = extract_attr( jsonml );
  if ( attrs ) {
    jsonml[ 1 ] = {};
    for ( i in attrs ) {
      jsonml[ 1 ][ i ] = attrs[ i ];
    }
    attrs = jsonml[ 1 ];
  }

  // basic case
  if ( typeof jsonml === "string" ) {
    return jsonml;
  }

  // convert this node
  switch ( jsonml[ 0 ] ) {
    case "header":
      jsonml[ 0 ] = "h" + jsonml[ 1 ].level;
      delete jsonml[ 1 ].level;
      break;
    case "bulletlist":
      jsonml[ 0 ] = "ul";
      break;
    case "numberlist":
      jsonml[ 0 ] = "ol";
      break;
    case "listitem":
      jsonml[ 0 ] = "li";
      break;
    case "para":
      jsonml[ 0 ] = "p";
      break;
    case "markdown":
      jsonml[ 0 ] = "html";
      if ( attrs ) delete attrs.references;
      break;
    case "code_block":
      jsonml[ 0 ] = "pre";
      i = attrs ? 2 : 1;
      var code = [ "code" ];
      code.push.apply( code, jsonml.splice( i ) );
      jsonml[ i ] = code;
      break;
    case "inlinecode":
      jsonml[ 0 ] = "code";
      break;
    case "img":
      jsonml[ 1 ].src = jsonml[ 1 ].href;
      delete jsonml[ 1 ].href;
      break;
    case "linebreak":
      jsonml[ 0 ] = "br";
    break;
    case "link":
      jsonml[ 0 ] = "a";
      break;
    case "link_ref":
      jsonml[ 0 ] = "a";

      // grab this ref and clean up the attribute node
      var ref = references[ attrs.ref ];

      // if the reference exists, make the link
      if ( ref ) {
        delete attrs.ref;

        // add in the href and title, if present
        attrs.href = ref.href;
        if ( ref.title ) {
          attrs.title = ref.title;
        }

        // get rid of the unneeded original text
        delete attrs.original;
      }
      // the reference doesn't exist, so revert to plain text
      else {
        return attrs.original;
      }
      break;
    case "img_ref":
      jsonml[ 0 ] = "img";

      // grab this ref and clean up the attribute node
      var ref = references[ attrs.ref ];

      // if the reference exists, make the link
      if ( ref ) {
        delete attrs.ref;

        // add in the href and title, if present
        attrs.src = ref.href;
        if ( ref.title ) {
          attrs.title = ref.title;
        }

        // get rid of the unneeded original text
        delete attrs.original;
      }
      // the reference doesn't exist, so revert to plain text
      else {
        return attrs.original;
      }
      break;
  }

  // convert all the children
  i = 1;

  // deal with the attribute node, if it exists
  if ( attrs ) {
    // if there are keys, skip over it
    for ( var key in jsonml[ 1 ] ) {
      i = 2;
    }
    // if there aren't, remove it
    if ( i === 1 ) {
      jsonml.splice( i, 1 );
    }
  }

  for ( ; i < jsonml.length; ++i ) {
    jsonml[ i ] = arguments.callee( jsonml[ i ], references, options );
  }

  return jsonml;
}


// merges adjacent text nodes into a single node
function merge_text_nodes( jsonml ) {
  // skip the tag name and attribute hash
  var i = extract_attr( jsonml ) ? 2 : 1;

  while ( i < jsonml.length ) {
    // if it's a string check the next item too
    if ( typeof jsonml[ i ] === "string" ) {
      if ( i + 1 < jsonml.length && typeof jsonml[ i + 1 ] === "string" ) {
        // merge the second string into the first and remove it
        jsonml[ i ] += jsonml.splice( i + 1, 1 )[ 0 ];
      }
      else {
        ++i;
      }
    }
    // if it's not a string recurse
    else {
      arguments.callee( jsonml[ i ] );
      ++i;
    }
  }
}

} )( (function() {
  if ( typeof exports === "undefined" ) {
    window.markdown = {};
    return window.markdown;
  }
  else {
    return exports;
  }
} )() );
/* ===================================================
 * bootstrap-suggest.js v1.0.0
 * http://github.com/lodev09/bootstrap-suggest
 * ===================================================
 * Copyright 2014 Jovanni Lo
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ========================================================== */

(function ($) {

	"use strict"; // jshint ;_;

	var Suggest = function(el, key, options) {
		var that = this;

		this.$element = $(el);
		this.$items = undefined;
		this.options = $.extend(true, {}, $.fn.suggest.defaults, options, this.$element.data(), this.$element.data('options'));
		this.key = key;
		this.isShown = false;
		this.query = '';
		this._queryPos = [];
		this._keyPos = -1;

		this.$dropdown = $('<div />', {
			class: 'dropdown suggest',
			html: $('<ul />', {class: 'dropdown-menu', role: 'menu'}),
			'data-key': this.key
		});

		this.load();

	};

	Suggest.prototype = {
		__setListener: function() {
			this.$element
				.on('suggest.show', $.proxy(this.options.onshow, this))
				.on('suggest.select', $.proxy(this.options.onselect, this))
				.on('suggest.lookup', $.proxy(this.options.onlookup, this))
				.on('keypress', $.proxy(this.__keypress, this))
				.on('keyup', $.proxy(this.__keyup, this));

			return this;
		},

		__getCaretPos: function(posStart) {
			// https://github.com/component/textarea-caret-position/blob/master/index.js

			// The properties that we copy into a mirrored div.
			// Note that some browsers, such as Firefox,
			// do not concatenate properties, i.e. padding-top, bottom etc. -> padding,
			// so we have to do every single property specifically.
			var properties = [
			  'direction',  // RTL support
			  'boxSizing',
			  'width',  // on Chrome and IE, exclude the scrollbar, so the mirror div wraps exactly as the textarea does
			  'height',
			  'overflowX',
			  'overflowY',  // copy the scrollbar for IE

			  'borderTopWidth',
			  'borderRightWidth',
			  'borderBottomWidth',
			  'borderLeftWidth',

			  'paddingTop',
			  'paddingRight',
			  'paddingBottom',
			  'paddingLeft',

			  // https://developer.mozilla.org/en-US/docs/Web/CSS/font
			  'fontStyle',
			  'fontVariant',
			  'fontWeight',
			  'fontStretch',
			  'fontSize',
			  'fontSizeAdjust',
			  'lineHeight',
			  'fontFamily',

			  'textAlign',
			  'textTransform',
			  'textIndent',
			  'textDecoration',  // might not make a difference, but better be safe

			  'letterSpacing',
			  'wordSpacing'
			];

			var isFirefox = !(window.mozInnerScreenX == null);

			var getCaretCoordinatesFn = function (element, position, recalculate) {
			  // mirrored div
			  var div = document.createElement('div');
			  div.id = 'input-textarea-caret-position-mirror-div';
			  document.body.appendChild(div);

			  var style = div.style;
			  var computed = window.getComputedStyle? getComputedStyle(element) : element.currentStyle;  // currentStyle for IE < 9

			  // default textarea styles
			  style.whiteSpace = 'pre-wrap';
			  if (element.nodeName !== 'INPUT')
			    style.wordWrap = 'break-word';  // only for textarea-s

			  // position off-screen
			  style.position = 'absolute';  // required to return coordinates properly
			  style.visibility = 'hidden';  // not 'display: none' because we want rendering

			  // transfer the element's properties to the div
			  properties.forEach(function (prop) {
			    style[prop] = computed[prop];
			  });

			  if (isFirefox) {
			    style.width = parseInt(computed.width) - 2 + 'px'  // Firefox adds 2 pixels to the padding - https://bugzilla.mozilla.org/show_bug.cgi?id=753662
			    // Firefox lies about the overflow property for textareas: https://bugzilla.mozilla.org/show_bug.cgi?id=984275
			    if (element.scrollHeight > parseInt(computed.height))
			      style.overflowY = 'scroll';
			  } else {
			    style.overflow = 'hidden';  // for Chrome to not render a scrollbar; IE keeps overflowY = 'scroll'
			  }

			  div.textContent = element.value.substring(0, position);
			  // the second special handling for input type="text" vs textarea: spaces need to be replaced with non-breaking spaces - http://stackoverflow.com/a/13402035/1269037
			  if (element.nodeName === 'INPUT')
			    div.textContent = div.textContent.replace(/\s/g, "\u00a0");

			  var span = document.createElement('span');
			  // Wrapping must be replicated *exactly*, including when a long word gets
			  // onto the next line, with whitespace at the end of the line before (#7).
			  // The  *only* reliable way to do that is to copy the *entire* rest of the
			  // textarea's content into the <span> created at the caret position.
			  // for inputs, just '.' would be enough, but why bother?
			  span.textContent = element.value.substring(position) || '.';  // || because a completely empty faux span doesn't render at all
			  div.appendChild(span);

			  var coordinates = {
			    top: span.offsetTop + parseInt(computed['borderTopWidth']),
			    left: span.offsetLeft + parseInt(computed['borderLeftWidth'])
			  };

			  document.body.removeChild(div);

			  return coordinates;
			}

			return getCaretCoordinatesFn(this.$element.get(0), posStart);
		},

		__keyup: function(e) {
			// don't query special characters
			// http://mikemurko.com/general/jquery-keycode-cheatsheet/


			var specialChars = [38, 40, 37, 39, 17, 18, 9, 16, 20, 91, 93, 36, 35, 45, 33, 34, 144, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 145, 19],
				$resultItems;

			switch (e.keyCode) {
				case 27:
					this.hide();
					return;
				case 13:
					return true;
			}


			if ($.inArray(e.keyCode, specialChars) !== -1) return true;

			var $el = this.$element,
				val = $el.val(),
				currentPos = $el.get(0).selectionStart;


			for (var i = currentPos; i >= 0; i--) {
				var subChar = $.trim(val.substring(i-1, i));
				if (!subChar) {
					this.hide();
					break;
				}

				if (subChar === this.key && $.trim(val.substring(i-2, i-1)) == '') {
					this.query = val.substring(i, currentPos);
					this._queryPos = [i, currentPos];
					this._keyPos = i;
					$resultItems = this.lookup(this.query);


					if ($resultItems.length) this.show();
					else this.hide();
					break;
				}
			}
		},

		__getVisibleItems: function() {
			return this.$items.not('.hidden');
		},

		__build: function() {
			var elems = [], _data, $item,
				$dropdown = this.$dropdown,
				that = this;

			if (typeof this.options.data == 'function') {
				_data = this.options.data();
			} else _data = this.options.data;

			if (_data && _data instanceof Array) {
				for (var i in _data) {
					if ($item = this.__mapItem(_data[i]))
						$dropdown.find('.dropdown-menu').append($item.addClass('hidden'));
				}
			}

			var blur = function(e) {
				that.hide();
			}

			this.$items = $dropdown.find('li:has(a)')
				.on('click', function(e) {
					e.preventDefault();
					that.__select($(this).index());
				})
				.on('mouseover', function(e) {
					that.$element.off('blur', blur);
				})
				.on('mouseout', function(e) {
					that.$element.on('blur', blur);
				});

			this.$element.before($dropdown)
				.on('blur', blur)
				.on('keydown', function(e) {
					var $visibleItems;
					if (that.isShown) {
						switch (e.keyCode) {
							case 13: // enter key
								$visibleItems = that.__getVisibleItems();
								$visibleItems.each(function(index) {
									if ($(this).is('.active'))
										that.__select($(this).index());
								});

								return false;
								break;
							case 40: // arrow down
								$visibleItems = that.__getVisibleItems();
								if ($visibleItems.last().is('.active')) return false;
								$visibleItems.each(function(index) {
									var $this = $(this),
										$next = $visibleItems.eq(index + 1);

									//if (!$next.length) return false;

									if ($this.is('.active')) {
										if (!$next.is('.hidden')) {
											$this.removeClass('active');
											$next.addClass('active');
										}
										return false;
									}
								});
								return false;
							case 38: // arrow up
								$visibleItems = that.__getVisibleItems();
								if ($visibleItems.first().is('.active')) return false;
								$visibleItems.each(function(index) {
									var $this = $(this),
										$prev = $visibleItems.eq(index - 1);

									//if (!$prev.length) return false;

									if ($this.is('.active')) {
										if (!$prev.is('.hidden')) {
											$this.removeClass('active');
											$prev.addClass('active');
										}
										return false;
									}
								})
								return false;
						}
					}
				});

		},

		__mapItem: function(dataItem) {
			var itemHtml, that = this,
				_item = {
					text: '',
					value: ''
				};

			if (this.options.map) {
				dataItem = this.options.map(dataItem);
				if (!dataItem) return false;
			}

			if (dataItem instanceof Object) {
				_item.text = dataItem.text || '';
				_item.value = dataItem.value || '';
			} else {
				_item.text = dataItem;
				_item.value = dataItem;
			}

			return $('<li />', {'data-value': _item.value}).html($('<a />', {
				href: '#',
				html: _item.text
			}));
		},

		__select: function(index) {
			var $el = this.$element,
				el = $el.get(0),
				val = $el.val(),
				item = this.get(index),
				setCaretPos = this._keyPos + item.value.length + 1;

			$el.val(val.slice(0, this._keyPos) + item.value + ' ' + val.slice(el.selectionStart));

			if (el.setSelectionRange) {
				el.setSelectionRange(setCaretPos, setCaretPos);
			} else if (el.createTextRange) {
				var range = el.createTextRange();
				range.collapse(true);
				range.moveEnd('character', setCaretPos);
				range.moveStart('character', setCaretPos);
				range.select();
			}

			$el.trigger($.extend({type: 'suggest.select'}, this), item);

			this.hide();
		},

		get: function(index) {
			var $item = this.$items.eq(index);
			return {
				text: $item.children('a:first').text(),
				value: $item.attr('data-value'),
				index: index,
				$element: $item
			};
		},

		lookup: function(q) {
			var options = this.options,
				that = this,
				$resultItems;

			this.$items.addClass('hidden');
			if (q != "") {
				this.$items.filter(function (index) {
					var $this = $(this),
						value = $this.find('a:first').text();

					if (!options.filter.casesensitive) {
						value = value.toLowerCase();
						q = q.toLowerCase();
					}

		            return value.indexOf(q) != -1;
		        }).slice(0, options.filter.limit).removeClass('hidden active');
		    } else this.$items.slice(0, options.filter.limit).removeClass('hidden active');

		    $resultItems = this.__getVisibleItems();
		    this.$element.trigger($.extend({type: 'suggest.lookup'}, this), [q, $resultItems]);

		    return $resultItems.eq(0).addClass('active');
		},

		load: function() {
			this.__setListener();
			this.__build();
		},

		hide: function() {
			this.$dropdown.removeClass('open');
			this.isShown = false;
			this.$items.removeClass('active');
			this._keyPos = -1;
      if(this.options.onhide){
        this.options.onhide();
      }
		},

		show: function() {
			var $el = this.$element,
				el = $el.get(0);

			if (!this.isShown) {
				var caretPos = this.__getCaretPos(this._keyPos);
				this.$dropdown
					.addClass('open')
					.find('.dropdown-menu').css({
						'top': caretPos.top - el.scrollTop + 'px',
						'left': caretPos.left - el.scrollLeft + 'px'
					});
				this.isShown = true;
				$el.trigger($.extend({type: 'suggest.show'}, this));
			}
		}
	};

	var old = $.fn.suggest;

	// .suggest( key [, options] )
	// .suggest( method [, options] )
	// .suggest( suggestions )
	$.fn.suggest = function(arg1) {
		var arg2 = arguments[1];

		var createSuggest = function(el, suggestions) {
			var newData = {};
			$.each(suggestions, function(keyChar, options) {
				var key =  keyChar.toString().charAt(0);
				newData[key] = new Suggest(el, key, typeof options == 'object' && options);
			});

			return newData;
		};

		return this.each(function() {
			var that = this,
				$this = $(this),
				data = $this.data('suggest'),
				suggestion = {};

			if (typeof arg1 == 'string') {
				if (arg1.length > 1 && data) {
					// arg1 as a method
					if (typeof data[arg1] != 'undefined') data[arg1](arg2);
				} else if (arg1.length == 1) {
					// arg1 as key
					if (arg2) {

						// inline data determined if it's an array
						suggestion[arg1] = arg2 instanceof Array ? {data: arg2} : arg2;
						if (!data) {
							$this.data('suggest', createSuggest(this, suggestion));
						} else if (data && !arg1 in data) {
							$this.data('suggest', $.extend(data, createSuggest(this, suggestion)));
						}
					}
				}
			} else {
				// arg1 contains set of suggestions
				if (!data) $this.data('suggest', createSuggest(this, arg1));
				else if (data) {
					$.each(arg1, function(key, value) {
						if (key in data == false) {
							suggestion[key] = value;
						}
					});

					$this.data('suggest', $.extend(data, createSuggest(that, suggestion)))
				}
			}
		});
	};

	$.fn.suggest.defaults = {
		data: [],
		map: undefined,
		filter: {
			casesensitive: false,
			limit: 5
		},

		// events hook
		onshow: function(e) {},
		onselect: function(e, item) {},
		onlookup: function(e, item) {}

	}

	$.fn.suggest.Constructor = Suggest;

	$.fn.suggest.noConflict = function () {
		$.fn.suggest = old;
		return this;
	}

}( jQuery ));

/*
 * to-markdown - an HTML to Markdown converter
 *
 * Copyright 2011, Dom Christie
 * Licenced under the MIT licence
 *
 */

var toMarkdown = function(string) {
  
  var ELEMENTS = [
    {
      patterns: 'p',
      replacement: function(str, attrs, innerHTML) {
        return innerHTML ? '\n\n' + innerHTML + '\n' : '';
      }
    },
    {
      patterns: 'br',
      type: 'void',
      replacement: '\n'
    },
    {
      patterns: 'h([1-6])',
      replacement: function(str, hLevel, attrs, innerHTML) {
        var hPrefix = '';
        for(var i = 0; i < hLevel; i++) {
          hPrefix += '#';
        }
        return '\n\n' + hPrefix + ' ' + innerHTML + '\n';
      }
    },
    {
      patterns: 'hr',
      type: 'void',
      replacement: '\n\n* * *\n'
    },
    {
      patterns: 'a',
      replacement: function(str, attrs, innerHTML) {
        var href = attrs.match(attrRegExp('href')),
            title = attrs.match(attrRegExp('title'));
        return href ? '[' + innerHTML + ']' + '(' + href[1] + (title && title[1] ? ' "' + title[1] + '"' : '') + ')' : str;
      }
    },
    {
      patterns: ['b', 'strong'],
      replacement: function(str, attrs, innerHTML) {
        return innerHTML ? '**' + innerHTML + '**' : '';
      }
    },
    {
      patterns: ['i', 'em'],
      replacement: function(str, attrs, innerHTML) {
        return innerHTML ? '_' + innerHTML + '_' : '';
      }
    },
    {
      patterns: 'code',
      replacement: function(str, attrs, innerHTML) {
        return innerHTML ? '`' + innerHTML + '`' : '';
      }
    },
    {
      patterns: 'img',
      type: 'void',
      replacement: function(str, attrs, innerHTML) {
        var src = attrs.match(attrRegExp('src')),
            alt = attrs.match(attrRegExp('alt')),
            title = attrs.match(attrRegExp('title'));
        return '![' + (alt && alt[1] ? alt[1] : '') + ']' + '(' + src[1] + (title && title[1] ? ' "' + title[1] + '"' : '') + ')';
      }
    }
  ];
  
  for(var i = 0, len = ELEMENTS.length; i < len; i++) {
    if(typeof ELEMENTS[i].patterns === 'string') {
      string = replaceEls(string, { tag: ELEMENTS[i].patterns, replacement: ELEMENTS[i].replacement, type:  ELEMENTS[i].type });
    }
    else {
      for(var j = 0, pLen = ELEMENTS[i].patterns.length; j < pLen; j++) {
        string = replaceEls(string, { tag: ELEMENTS[i].patterns[j], replacement: ELEMENTS[i].replacement, type:  ELEMENTS[i].type });
      }
    }
  }
  
  function replaceEls(html, elProperties) {
    var pattern = elProperties.type === 'void' ? '<' + elProperties.tag + '\\b([^>]*)\\/?>' : '<' + elProperties.tag + '\\b([^>]*)>([\\s\\S]*?)<\\/' + elProperties.tag + '>',
        regex = new RegExp(pattern, 'gi'),
        markdown = '';
    if(typeof elProperties.replacement === 'string') {
      markdown = html.replace(regex, elProperties.replacement);
    }
    else {
      markdown = html.replace(regex, function(str, p1, p2, p3) {
        return elProperties.replacement.call(this, str, p1, p2, p3);
      });
    }
    return markdown;
  }
  
  function attrRegExp(attr) {
    return new RegExp(attr + '\\s*=\\s*["\']?([^"\']*)["\']?', 'i');
  }
  
  // Pre code blocks
  
  string = string.replace(/<pre\b[^>]*>`([\s\S]*)`<\/pre>/gi, function(str, innerHTML) {
    innerHTML = innerHTML.replace(/^\t+/g, '  '); // convert tabs to spaces (you know it makes sense)
    innerHTML = innerHTML.replace(/\n/g, '\n    ');
    return '\n\n    ' + innerHTML + '\n';
  });
  
  // Lists

  // Escape numbers that could trigger an ol
  // If there are more than three spaces before the code, it would be in a pre tag
  // Make sure we are escaping the period not matching any character
  string = string.replace(/^(\s{0,3}\d+)\. /g, '$1\\. ');
  
  // Converts lists that have no child lists (of same type) first, then works it's way up
  var noChildrenRegex = /<(ul|ol)\b[^>]*>(?:(?!<ul|<ol)[\s\S])*?<\/\1>/gi;
  while(string.match(noChildrenRegex)) {
    string = string.replace(noChildrenRegex, function(str) {
      return replaceLists(str);
    });
  }
  
  function replaceLists(html) {
    
    html = html.replace(/<(ul|ol)\b[^>]*>([\s\S]*?)<\/\1>/gi, function(str, listType, innerHTML) {
      var lis = innerHTML.split('</li>');
      lis.splice(lis.length - 1, 1);
      
      for(i = 0, len = lis.length; i < len; i++) {
        if(lis[i]) {
          var prefix = (listType === 'ol') ? (i + 1) + ".  " : "*   ";
          lis[i] = lis[i].replace(/\s*<li[^>]*>([\s\S]*)/i, function(str, innerHTML) {
            
            innerHTML = innerHTML.replace(/^\s+/, '');
            innerHTML = innerHTML.replace(/\n\n/g, '\n\n    ');
            // indent nested lists
            innerHTML = innerHTML.replace(/\n([ ]*)+(\*|\d+\.) /g, '\n$1    $2 ');
            return prefix + innerHTML;
          });
        }
      }
      return lis.join('\n');
    });
    return '\n\n' + html.replace(/[ \t]+\n|\s+$/g, '');
  }
  
  // Blockquotes
  var deepest = /<blockquote\b[^>]*>((?:(?!<blockquote)[\s\S])*?)<\/blockquote>/gi;
  while(string.match(deepest)) {
    string = string.replace(deepest, function(str) {
      return replaceBlockquotes(str);
    });
  }
  
  function replaceBlockquotes(html) {
    html = html.replace(/<blockquote\b[^>]*>([\s\S]*?)<\/blockquote>/gi, function(str, inner) {
      inner = inner.replace(/^\s+|\s+$/g, '');
      inner = cleanUp(inner);
      inner = inner.replace(/^/gm, '> ');
      inner = inner.replace(/^(>([ \t]{2,}>)+)/gm, '> >');
      return inner;
    });
    return html;
  }
  
  function cleanUp(string) {
    string = string.replace(/^[\t\r\n]+|[\t\r\n]+$/g, ''); // trim leading/trailing whitespace
    string = string.replace(/\n\s+\n/g, '\n\n');
    string = string.replace(/\n{3,}/g, '\n\n'); // limit consecutive linebreaks to 2
    return string;
  }
  
  return cleanUp(string);
};

if (typeof exports === 'object') {
  exports.toMarkdown = toMarkdown;
}