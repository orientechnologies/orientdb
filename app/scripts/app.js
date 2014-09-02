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
    'ngTable',
    'filters',
    'rendering',
    'graph',
    'schema.controller',
    'duScroll',
    'ui.select2',
    'ngRoute',
    'ngAnimate',
    'angularSpectrumColorpicker'];


var App = angular.module('OrientDBStudioApp', deps);

App.config(function ($routeProvider, $httpProvider) {
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
        .when('/database/:database/users', {
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
            resolve: DatabaseResolve
        })
        .when('/database/:database/db/:tab', {
            templateUrl: 'views/database/configuration.html',
            controller: 'ConfigurationController',
            resolve: DatabaseResolve
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
App.run(function ($rootScope, $interval, DatabaseApi, Notification, Spinner) {
    $rootScope.$on('$routeChangeSuccess', function (event, currentRoute) {
        switch (currentRoute.templateUrl) {
            case 'views/login.html':
                $rootScope.bodyClass = 'landing-page';
                break;
            default:
                $rootScope.bodyClass = 'normal-page';
                break;
        }
        Notification.clear();
        NProgress.done();

    });
    $rootScope.$on("$routeChangeStart", function (event, next, current) {
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
    }, 3000);
})