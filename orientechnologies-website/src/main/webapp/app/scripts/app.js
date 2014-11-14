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
      .otherwise({
        redirectTo: '/'
      });

    $httpProvider.interceptors.push('oauthHttpInterceptor');
    RestangularProvider.setBaseUrl('/api/v1');


  });
angular.module('webappApp').factory('oauthHttpInterceptor', function () {
  return {
    request: function (config) {
      config.headers['X-AUTH-TOKEN'] = 'ea05530bd2e36f2caf082c0a8815a7125445fc4b';
      return config;
    }
  };
});


var API = "v1/"
var ORGANIZATION = 'romeshell';
