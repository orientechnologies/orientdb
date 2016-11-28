import 'angular-route'

import {DatabaseResolve, InstantDatabaseResolve, AgentResolve} from './resolvers';

import LoginTpl from './views/login.html';
import './views/header.html';
import './views/spinner.html';
import './views/notification.html';
import './views/widget/aside.html';
import './views/status.html';
import './translations/en-US/hint.json';

import './views/database/schema.html';
import './views/database/browse.html';
import './views/database/security.html';
import './views/database/graph/graph.html';

function routing($routeProvider, $httpProvider, $translateProvider, $translatePartialLoaderProvider) {
  $routeProvider
    .when('/', {
      templateUrl: LoginTpl,
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
    }).when('/database/:database/browse/editclass/:clazz', {
    templateUrl: 'views/database/editclass.html',
    controller: 'ClassEditController',
    resolve: DatabaseResolve
  })
    .when('/database/:database/schema/create/:clazz', {
      templateUrl: 'views/database/createRecord.html',
      controller: 'CreateController',
      resolve: DatabaseResolve
    }).when('/database/:database/schema/editclass/:clazz', {
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
      resolve: AgentResolve
    })
    .when('/dashboard/:tab', {
      templateUrl: 'views/server/dashboard.html',
      controller: 'ServerDashboardController',
      resolve: AgentResolve
    })
    .when('/dashboard/:tab/:server', {
      templateUrl: 'views/server/dashboard.html',
      controller: 'ServerDashboardController',
      resolve: AgentResolve
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
          Notification.push({content: rejection.data, error: true, autoHide: false});
        }
        return $q.reject(rejection);
      }
    };
  });

}


var checkError401 = function (data) {

  var valid = true;
  if (typeof  data == 'string') {
    valid = data != "Logged out";
  } else {
    valid = data.errors[0].content != "Logged out"
  }
  return valid;

}
routing.$inject = ['$routeProvider', '$httpProvider', '$translateProvider', '$translatePartialLoaderProvider'];

export  default routing;
