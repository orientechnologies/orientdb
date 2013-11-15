'use strict';

angular.module('MonitorApp', ['ngI18n', ,'messages.controller','workbench-logs.controller', 'workbench-events.controller','login.services', 'monitor.services', 'ui-nvd3', 'ngMoment', 'OFilter', 'ui.select2', '$strap.directives', 'monitor.directive', 'orientdb.directives','ui.codemirror','ngGrid','bootstrap.tabset','message.services'])
    .config(function ($routeProvider) {
        $routeProvider
            .when('/', {
                templateUrl: 'views/main.html',
                controller: 'ServerMonitorController'
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
            .when('/dashboard/logjava/:rid', {
                templateUrl: 'views/server/logjava.html',
                controller: 'LogsJavaController'
            })
            .when('/dashboard/events/:rid', {
                templateUrl: 'views/server/events.html',
                controller: 'EventsController'
            })
            .when('/dashboard/messages/:rid', {
                templateUrl: 'views/server/messages.html',
                controller: 'MessagesController'
            })
            .otherwise({
                redirectTo: '/'
            });
    });


$('*', '.modal').on('show', function(e) {e.stopPropagation();}).on('hide', function(e) {e.stopPropagation();});
$('*', '.popover').on('show', function(e) {e.stopPropagation();}).on('hide', function(e) {console.log(e); e.stopPropagation();});

$('.popover').on("hide", function (e) {

    e.stopPropagation();
});