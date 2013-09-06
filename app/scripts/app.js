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
    'notification.controller',
    'configuration.controller',
    '$strap.directives',
    'ui.codemirror',
    'LocalStorageModule',
    'filters',
    'rendering', 'schema.controller'];


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
        .when('/database/:database/configuration', {
            templateUrl: 'views/database/configuration.html',
            controller: 'ConfigurationController',
            resolve: DatabaseResolve
        })
        .when('/database/:database/configuration/:tab', {
            templateUrl: 'views/database/configuration.html',
            controller: 'ConfigurationController',
            resolve: DatabaseResolve
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
});
