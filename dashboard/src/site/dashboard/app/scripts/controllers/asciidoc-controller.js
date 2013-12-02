var module = angular.module('MonitorApp');
module.controller('AsciidocController', function ($scope, $location, $routeParams, Monitor) {


    $scope.nav = $routeParams.helpid || 'help';
    $scope.template = 'views/server/asciidoc/' + $scope.nav + ".html";




});
