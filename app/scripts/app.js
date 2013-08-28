'use strict';


var deps = ['header.controller',
'breadcrumb.controller',
'spinner.controller',
'login.controller',
'database.controller',
'document.controller',
'vertex.controller',
'notification.controller',
'$strap.directives',
'ui.codemirror',
'ui.select2',
'ngTable',
'LocalStorageModule',
'filters',
'rendering','schema.controller'];



var App = angular.module('OrientDBStudioApp', deps);

App.config(function ($routeProvider) {
  $routeProvider
  .when('/', {
    templateUrl: 'views/login.html',
    controller: 'LoginController'
  })
  .when('/database/:database/browse', {
    templateUrl: 'views/database/browse.html',
    controller: 'BrowseController',
    resolve : DatabaseResolve
  })
  .when('/database/:database/browse/:query', {
    templateUrl: 'views/database/browse.html',
    controller: 'BrowseController',
    resolve : DatabaseResolve
  })
   .when('/database/:database/schema', {
    templateUrl: 'views/database/schema.html',
    controller: 'SchemaController',
    resolve : DatabaseResolve
  })
  .when('/database/:database/browse/edit/:rid', {
    templateUrl: 'views/database/edit.html',
    controller: 'EditController',
    resolve : DatabaseResolve
  })
  .when('/database/:database/browse/create/:clazz', {
    templateUrl: 'views/database/createRecord.html',
    controller: 'CreateController',
    resolve : DatabaseResolve
  }).
  when('/database/:database/browse/editclass/:clazz', {
    templateUrl: 'views/database/editclass.html',
    controller: 'ClassEditController',
    resolve : DatabaseResolve
  })
  .when('/404' , {
    templateUrl: 'views/404.html'
  })
  .otherwise({
    redirectTo: '/'
  });
});
