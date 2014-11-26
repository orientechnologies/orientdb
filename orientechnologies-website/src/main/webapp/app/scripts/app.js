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
    'mgcrea.ngStrap',
    'ngUtilFilters'
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
angular.module('webappApp').factory('oauthHttpInterceptor', function () {
  return {
    request: function (config) {
      config.headers['X-AUTH-TOKEN'] = '712269688bc391fba83280bd498da00bc0dc7507';
      return config;
    }
  };
});


var API = "v1/"
var ORGANIZATION = 'organizationwolf';
