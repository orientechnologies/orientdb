'use strict';

angular.module('MonitorApp', ['ngI18n', 'monitor.services', 'ui-nvd3', 'ngMoment', 'OFilter', 'ui.select2', '$strap.directives', 'monitor.directive', 'orientdb.directives'])
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
                controller: 'ServerMonitorController'
            })


            .otherwise({
                redirectTo: '/'
            });
    });
