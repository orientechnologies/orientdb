'use strict';

angular.module('MonitorApp')
    .controller('NavController', function ($scope,$routeParams, $location, Monitor) {


        $scope.menus = [
            {name: 'dashboard', i18n: 'dashboard', link: '#/dashboard/', icon: 'icon-dashboard' },
            {name: 'general', i18n: 'dashboard.general', link: '#/dashboard/general/' + $scope.rid, icon: 'icon-cogs' },
            {name: 'query', i18n: 'dashboard.query', link: '#/dashboard/query/' + $scope.rid, icon: 'icon-rocket' },
            {name: 'metrics', i18n: 'dashboard.metrics', link: '#/dashboard/metrics/' + $scope.rid, icon: 'icon-bar-chart' },
            {name: 'log', i18n: 'dashboard.log', link: '#/dashboard/log/' + $scope.rid, icon: 'icon-bug' },
            {name: 'logjava', i18n: 'dashboard.javalog', link: '#/dashboard/logjava/' + $scope.rid, icon: 'icon-bug' },
            {name: 'events', i18n: 'dashboard.events', link: '#/dashboard/events/' + $scope.rid, icon: 'icon-warning-sign' }

        ]

        Monitor.getServers(function (data) {
            $scope.servers = data.result;
            $scope.rid = $routeParams.server || ($scope.servers[0] ? $scope.servers[0]['@rid'] : "");
            $scope.rid = $scope.rid.replace("#", '');

        });

        $scope.setSelected = function () {

            $scope.menus.forEach(function (element, index, array) {
                var find = $location.path().indexOf("/" + element.name.toLowerCase());
                if (find != -1) {
                    $scope.selectedMenu = element;
                }

            });
        }
        $scope.navigate = function (menu) {
            var menuEntry = menu.name != 'dashboard' ? (menu.name + '/' + $scope.rid) : "";
            $location.path('/dashboard/' + menuEntry );
        }
        $scope.$on('$routeChangeSuccess', function (scope, next, current) {
            $scope.setSelected();
        });
        $scope.$watch('rid', function (data) {
            if ($scope.menus && $scope.selectedMenu) {
                $scope.navigate($scope.selectedMenu);
            }
        })
    });