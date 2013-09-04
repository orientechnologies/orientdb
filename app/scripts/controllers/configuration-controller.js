/**
 * Created with JetBrains WebStorm.
 * User: erisa
 * Date: 04/09/13
 * Time: 17.13
 * To change this template use File | Settings | File Templates.
 */

var configModule = angular.module('configuration.controller',[]);
configModule.controller("ConfigurationController",['$scope','$routeParams','$location','DatabaseApi',function($scope,$routeParams,$location,DatabaseApi){

    $scope.active = $routeParams.tab || "conn";
    $scope.db = $routeParams.database;

    $scope.tabs = ['uml','structure','allocation','security','configuration','import-export'];

    $scope.getTemplate=function(tab) {
        return 'views/database/config/' + tab +'.html';
    }
    $scope.exportDatabase = function(){
        DatabaseApi.exportDatabase($scope.db);
    }
}]);
