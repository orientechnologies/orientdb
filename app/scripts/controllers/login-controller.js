angular.module('login.controller',['database.services']).controller("LoginController",['$scope','$routeParams','$location','$modal','$q','Database','DatabaseApi',function($scope,$routeParams,$location,$modal,$q,Database,DatabaseApi){

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
	$scope.createNew = function() {
		modalScope = $scope.$new(true);	
		modalScope.stype = "local";
		modalScope.types = ['document','graph']
		modalScope.createNew = function(){
			DatabaseApi.createDatabase(modalScope.name,modalScope.type,modalScope.stype,modalScope.username,modalScope.password,function(data){
				$scope.databases.push(modalScope.name);
				modalScope.hide();
			});
		}
		var modalPromise = $modal({template: '/views/database/newDatabase.html', scope: modalScope});
		$q.when(modalPromise).then(function(modalEl) {
			modalEl.modal('show');
		});
	}
}]);