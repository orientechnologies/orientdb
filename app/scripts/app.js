'use strict';


var deps = ['header.controller',
  'breadcrumb.controller',
  'spinner.controller',
  'login.controller',
  'database.controller',
  'document.controller',
  'server.controller',
  'vertex.controller',
  'function.controller',
  'users.controller',
  'aside.controller',
  'notification.controller',
  'configuration.controller',
  'mgcrea.ngStrap',
  'ui.codemirror',
  'LocalStorageModule',
  'aside.services',
  'graph.services',
  'icon.services',
  'history.services',
  'browse.services',
  'ee.services',
  'ee.controller',
  'bootstrap.tabset',
  'ngTable',
  'filters',
  'rendering',
  'graph',
  'schema.controller',
  'duScroll',
  'ui.select',
  'ngRoute',
  'ngAnimate',
  'ngSanitize',
  'angularSpectrumColorpicker',
  'pascalprecht.translate',
  'ngTagsInput',
  'frapontillo.bootstrap-switch',
  'datatables',
  'smart-table'];


var App = angular.module('OrientDBStudioApp', deps);

App.config(function ($routeProvider, $httpProvider, $translateProvider, $translatePartialLoaderProvider) {
  $routeProvider
    .when('/', {
      templateUrl: 'views/login.html',
      controller: 'LoginController'
    })
    .when('/database/:database/browse', {
      templateUrl: 'views/database/browse.html',
      controller: 'BrowseController',
      resolve: DatabaseResolve
    })
    .when('/database/:database/browse/:query', {
      templateUrl: 'views/database/browse.html',
      controller: 'BrowseController',
      resolve: DatabaseResolve
    })
    .when('/database/:database/schema', {
      templateUrl: 'views/database/schema.html',
      controller: 'SchemaController',
      resolve: InstantDatabaseResolve
    })
    .when('/database/:database/schema/indexes', {
      templateUrl: 'views/database/index/indexMain.html',
      controller: 'IndexesController',
      resolve: DatabaseResolve
    })
    .when('/database/:database/browse/edit/:rid', {
      templateUrl: 'views/database/edit.html',
      controller: 'EditController',
      resolve: DatabaseResolve
    })
    .when('/database/:database/functions', {
      templateUrl: 'views/database/functions.html',
      controller: 'FunctionController',
      resolve: DatabaseResolve
    })
    .when('/database/:database/profiler', {
      templateUrl: 'views/database/profiler.html',
      controller: 'ProfilerController',
      resolve: DatabaseResolve
    })
    .when('/database/:database/auditing', {
      templateUrl: 'views/database/auditing.html',
      controller: 'AuditingController',
      resolve: DatabaseResolve
    })
    .when('/database/:database/security', {
      templateUrl: 'views/database/security.html',
      controller: 'SecurityController',
      resolve: DatabaseResolve
    })
    .when('/database/:database/security/:tab', {
      templateUrl: 'views/database/security.html',
      controller: 'SecurityController',
      resolve: DatabaseResolve
    })
    .when('/database/:database/browse/create/:clazz', {
      templateUrl: 'views/database/createRecord.html',
      controller: 'CreateController',
      resolve: DatabaseResolve
    }).
    when('/database/:database/browse/editclass/:clazz', {
      templateUrl: 'views/database/editclass.html',
      controller: 'ClassEditController',
      resolve: DatabaseResolve
    })
    .when('/database/:database/schema/create/:clazz', {
      templateUrl: 'views/database/createRecord.html',
      controller: 'CreateController',
      resolve: DatabaseResolve
    }).
    when('/database/:database/schema/editclass/:clazz', {
      templateUrl: 'views/database/editclass.html',
      controller: 'ClassEditController',
      resolve: DatabaseResolve
    })
    .when('/database/:database/db', {
      templateUrl: 'views/database/configuration.html',
      controller: 'ConfigurationController',
      resolve: InstantDatabaseResolve
    })
    .when('/database/:database/db/:tab', {
      templateUrl: 'views/database/configuration.html',
      controller: 'ConfigurationController',
      resolve: InstantDatabaseResolve
    })
    .when('/database/:database/graph', {
      templateUrl: 'views/database/graph/graph.html',
      controller: 'GraphController',
      resolve: DatabaseResolve
    })
    .when('/database/:database/graph/:q', {
      templateUrl: 'views/database/graph/graph.html',
      controller: 'GraphController',
      resolve: DatabaseResolve
    })
    .when('/dashboard', {
      templateUrl: 'views/server/dashboard.html',
      controller: 'ServerDashboardController',
      resolve : AgentResolve
    })
    .when('/dashboard/:tab', {
      templateUrl: 'views/server/dashboard.html',
      controller: 'ServerDashboardController',
      resolve : AgentResolve
    })
    .when('/server', {
      templateUrl: 'views/server/info.html',
      controller: 'ServerController'
    })
    .when('/server/:tab', {
      templateUrl: 'views/server/info.html',
      controller: 'ServerController'
    })
    .when('/404', {
      templateUrl: 'views/404.html'
    })
    .otherwise({
      redirectTo: '/'
    });

  $translateProvider.useLoader('$translatePartialLoader', {
    urlTemplate: 'translations/{lang}/{part}.json'
  });

  $translatePartialLoaderProvider.addPart('hint');

  $translateProvider.preferredLanguage('en-US');
  $httpProvider.interceptors.push(function ($q, Notification, $rootScope) {
    return {
      responseError: function (rejection) {


        if (rejection.status == 0) {
          Notification.clear();
          $rootScope.$broadcast("server:down");

        } else if (rejection.status == 401 && checkError401(rejection.data)) {
          console.log(rejection.data);
          Notification.push({content: rejection.data, error: true, autoHide: false});
        }
        return $q.reject(rejection);
      }
    };
  });

});
var POLLING = 2000;
App.run(function ($rootScope, $interval, DatabaseApi, Notification, Spinner, $templateCache, Aside) {
  $rootScope.$on('$routeChangeSuccess', function (event, currentRoute, oldRoute) {
    switch (currentRoute.templateUrl) {
      case 'views/login.html':
        $rootScope.bodyClass = 'landing-page';
        break;
      default:
        $rootScope.bodyClass = 'normal-page';
        break;
    }
    if (currentRoute.$$route.controller === 'ServerDashboardController') {
      $rootScope.$emit("servermgmt:open");
    } else {
      $rootScope.$emit("servermgmt:close");
    }
    if (oldRoute && currentRoute.originalPath != oldRoute.originalPath) {
      Notification.clear();
    }
    NProgress.done();
  });
  $rootScope.$on("$routeChangeStart", function (event, next, current) {
    Aside.destroy();
    NProgress.start();
    NProgress.set(0.2);
    NProgress.set(0.4);

  })

  $interval(function () {
    DatabaseApi.listDatabases().then(function (data) {
      $rootScope.$broadcast("server:up");
    }, function error(data) {
      $rootScope.$broadcast("server:down");
    })
  }, 10000);

  $templateCache.put('popover/popover.tpl.html', '<div class="popover"><div class="arrow"></div><h3 class="popover-title" ng-bind="title" ng-show="title"></h3><div class="popover-content" ng-bind-html="content"></div></div>');
})

var checkError401 = function (data) {

  var valid = true;
  if (typeof  data == 'string') {
    valid = data != "Logged out";
  } else {
    valid = data.errors[0].content != "Logged out"
  }
  return valid;

}
$('body').on('keyup', function (e) {

  if (e.keyCode == 27) {
    $('.modal-backdrop').click()
  }
})
