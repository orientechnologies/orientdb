'use strict';

angular.module('MonitorApp', ['ngI18n', 'workbench-logs.controller', 'workbench-events.controller', 'monitor.services', 'ui-nvd3', 'ngMoment', 'OFilter', 'ui.select2', '$strap.directives', 'monitor.directive', 'orientdb.directives'])
    .config(function ($routeProvider) {
        $routeProvider
            .when('/', {
                templateUrl: 'views/login.html',
                controller: 'LoginController'
            })
            .when('/login', {
                templateUrl: 'views/login.html',
                controller: 'LoginController'
            })
            .when('/dashboard', {
                templateUrl: 'views/main.html',
                controller: 'ServerMonitorController'
            })
            .when('/dashboard/:nav', {
                templateUrl: 'views/main.html',
                controller: 'ServerMonitorController'
            })
            .when('/dashboard/:nav/:server', {
                templateUrl: 'views/main.html',
                controller: 'ServerMonitorController'
            })
            .when('/dashboard/:nav/:server/:db', {
                templateUrl: 'views/main.html',
                controller: 'ServerMonitorController'
            })

            .when('/server/:rid', {
                templateUrl: 'views/server/main.html',
                controller: 'ServerMonitorController'
            })
            .when('/server/:rid/:tab', {
                templateUrl: 'views/server/main.html',
                controller: 'ServerMonitorController'
            })
            .when('/dashboard/log/:rid', {
                templateUrl: 'views/server/log.html',
                controller: 'LogsController'
            })
            .when('/dashboard/logjava/:rid', {
                templateUrl: 'views/server/logjava.html',
                controller: 'LogsJavaController'
            })
            .when('/dashboard/events/:rid', {
                templateUrl: 'views/server/events.html',
                controller: 'EventsController'
            })
            .otherwise({
                redirectTo: '/'
            });
    });
