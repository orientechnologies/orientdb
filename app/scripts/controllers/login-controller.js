angular.module('login.controller',['database.services']).controller("LoginController",['$scope','$routeParams','$location','Database','DatabaseApi',function($scope,$routeParams,$location,Database,DatabaseApi){

	$scope.server = "http://localhost:2480"


	DatabaseApi.listDatabases(function(data){
		$scope.databases = data.databases;
	});


	$scope.connect = function(){
		DatabaseApi.connect($scope.database,$scope.username,$scope.password,function(){
			var currentDb = DatabaseApi.get({database : $scope.database},function(){
				Database.current =  currentDb;
				Database.username = $scope.username;
				Database.dbName = $scope.database;
				$location.path("/database/" + $scope.database + "/browse");
			});
	
		});
	}
}]);