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
    'scroll'
  ])
  .config(function ($routeProvider, $httpProvider, RestangularProvider) {
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

    $httpProvider.interceptors.push(function ($q, $location) {
      return {
        responseError: function (rejection) {

          if (rejection.status == 401 || rejection.status == 403) {
            $location.path("/login")
          }
          return $q.reject(rejection);
        }
      };
    });
  });
angular.module('webappApp').factory('oauthHttpInterceptor', function ($cookies, AccessToken) {
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
});


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

//WEBSOCKET = 'ws://www.prjhub.com/chat';
//var WEBSOCKET = 'ws://prjhub.com/chat'
//var WEBSOCKET = 'ws://localhost:8080/chat'
