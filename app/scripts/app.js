'use strict';


var deps = ['header.controller','login.controller','database.controller','$strap.directives'];
angular.module('OrientDBStudioApp', deps)
  .config(function ($routeProvider) {
    $routeProvider
      .when('/', {
        templateUrl: 'views/login.html',
        controller: 'LoginController'
      })
      .when('/database/:database', {
        templateUrl: 'views/database.html',
        controller: 'DatabaseController'
      })
      .otherwise({
        redirectTo: '/'
      });
  });
