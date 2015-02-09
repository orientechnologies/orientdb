'use strict';

var app = angular.module('MonitorApp',
    ['ngI18n',
        'messages.controller',
        'workbench-logs.controller',
        'workbench-events.controller',
        'login.services',
        'monitor.services',
        'ui-nvd3',
        'ngMoment',
        'OFilter',
        'ui.select2',
        '$strap.directives',
        'monitor.directive',
        'monitor.gdirective',
        'orientdb.directives',
        'ui.codemirror',
        'ngGrid',
        'bootstrap.tabset',
        'message.services',
        'spinner.controller',
        'spinner.services',
        'ngRoute',
        'ngAnimate',
        'angularLocalStorage',
        'toggle-switch'
    ]
);
app.config(function ($routeProvider) {
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
        .when('/dashboard/cluster/:cluster/:db', {
            templateUrl: 'views/server/cluster.html',
            controller: 'ClusterMainController',
            reloadOnSearch: false
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
        }).when('/help', {
            templateUrl: 'views/server/asciidoc/introduction.html'
        }).when('/gettingstarted', {
            templateUrl: 'views/server/asciidoc/gettingstarted.html'
        })
        .when('/help/:helpid', {
            templateUrl: 'views/server/asciidoc/asciidoc.html',
            controller: 'AsciidocController'
        })
        .otherwise({
            redirectTo: '/'
        });
})
;

app.config(function ($httpProvider) {
    $httpProvider.interceptors.push(function ($rootScope, $location, $q, storage) {
        return {
            'request': function (request) {
                // if we're not logged-in to the AngularJS app, redirect to login page


                if (!$rootScope.loggedIn) {
                    var logged = JSON.parse(storage.get("login"));
                    if (logged) {
                        $rootScope.loggedIn = logged;
                    }
                }
                if (!$rootScope.loggedIn && $location.path() != '/login') {
                    $location.path('/login');
                }
                if ($location.path() == '/login' && $rootScope.loggedIn) {
                    $location.path('/dashboard');
                }
                return request;
            },
            'responseError': function (rejection) {
                // if we're not logged-in to the web service, redirect to login page
                if (rejection.status === 401 && $location.path() != '/login') {
                    $rootScope.loggedIn = false;
                    $location.path('/login');
                }
                return $q.reject(rejection);
            }
        };
    });
});
$('*', '.modal').on('show',function (e) {
    e.stopPropagation();
}).on('hide', function (e) {
        e.stopPropagation();
    });
$('*', '.popover').on('show',function (e) {
    e.stopPropagation();
}).on('hide', function (e) {
        console.log(e);
        e.stopPropagation();
    });

$('.popover').on("hide", function (e) {

    e.stopPropagation();
});
app.run(['$route', '$rootScope', '$location', function ($route, $rootScope, $location) {
    var original = $location.path;
    $location.path = function (path, reload) {
        if (reload === false) {
            var lastRoute = $route.current;
            var un = $rootScope.$on('$locationChangeSuccess', function () {
                $route.current = lastRoute;
                un();
            });
        }
        return original.apply($location, [path]);
    };
}])
