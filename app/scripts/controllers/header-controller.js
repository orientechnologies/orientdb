angular.module('header.controller',['database.services']).controller("HeaderController",['$scope','$routeParams','$http','$location','Database',function($scope,$routeParams,$http,$location,Database){
	$scope.database = Database;
	$scope.logout = function(){
		Database.current = null;
		$http.defaults.headers.common['Authorization'] = null;
		$location.path("/");
	}
}]);