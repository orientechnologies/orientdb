'use strict';


var deps = ['header.controller','login.controller','database.controller','document.controller','$strap.directives','ui.codemirror'];
angular.module('OrientDBStudioApp', deps)
  .config(function ($routeProvider) {
    $routeProvider
      .when('/', {
        templateUrl: 'views/login.html',
        controller: 'LoginController'
      })
      .when('/database/:database/:action', {
        templateUrl: 'views/database/database.html',
        controller: 'DatabaseController'
      })
      .when('/database/:database/browse/:rid', {
        templateUrl: 'views/database/editRecord.html',
        controller: 'DocumentController'
      })
      .otherwise({
        redirectTo: '/'
      });
  });
