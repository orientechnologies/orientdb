angular.module('login.controller',['database.services']).controller("LoginController",['$scope','$routeParams','$location','Database','DatabaseApi',function($scope,$routeParams,$location,Database,DatabaseApi){

	$scope.server = "http://localhost:2480"

	if(Database.isConnected()) {
		$location.path("/database/" + Database.getName() + "/browse");
	}
	DatabaseApi.listDatabases(function(data){
		$scope.databases = data.databases;
	});
	$scope.connect = function(){	
		Database.connect($scope.database,$scope.username,$scope.password,function(){
			$location.path("/database/" + $scope.database + "/browse");
		});
	}
}]);