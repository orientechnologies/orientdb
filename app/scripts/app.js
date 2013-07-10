'use strict';


var deps = ['header.controller',
'login.controller',
'database.controller',
'document.controller',
'notification.controller',
'$strap.directives',
'ui.codemirror',
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
  .when('/database/:database/:action', {
    templateUrl: 'views/database/database.html',
    controller: 'DatabaseController',
    resolve : DatabaseResolve
  })
  .when('/database/:database/browse/edit/:rid', {
    templateUrl: 'views/database/editRecord.html',
    controller: 'DocumentEditController',
    resolve : DatabaseResolve
  })
  .when('/database/:database/browse/create/:clazz', {
    templateUrl: 'views/database/createRecord.html',
    controller: 'DocumentCreateController',
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
