angular.module('database.controller',['database.services']).controller("DatabaseController",['$scope','$routeParams','Database','CommandApi',function($scope,$routeParams,Database,CommandApi){

	$scope.database = Database;
	$scope.limit = 20;
	$scope.query = function(){
		CommandApi.queryText({database : $routeParams.database, language : 'sql', text : $scope.queryText, limit : $scope.limit},function(data){
			$scope.headers = Object.keys(data.result[0]);
			$scope.results = data.result;
		});
	}
}]);