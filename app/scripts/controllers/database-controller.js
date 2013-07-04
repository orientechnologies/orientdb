var dbModule = angular.module('database.controller',['database.services']);
dbModule.controller("DatabaseController",['$scope','$routeParams','$location','Database','CommandApi',function($scope,$routeParams,$location,Database,CommandApi){

	$scope.database = Database;
	$scope.action = $routeParams.action;
	$scope.menus = [{ name : "browse"},{ name : "schema"},{ name : "users"}];

	$scope.getPartials = function(action){
		return "/views/database/" + action + ".html";
	}
}]);
dbModule.controller("BrowseController",['$scope','$routeParams','$location','Database','CommandApi',function($scope,$routeParams,$location,Database,CommandApi){
	$scope.database = Database;
	$scope.limit = 20;
	$scope.editorOptions = {
        lineWrapping : true,
        lineNumbers: true,
        readOnly: false,
        mode: 'text/x-sql',
        extraKeys: {
    		"Ctrl-Enter": function(instance) { $scope.query() }
  		}
    };
	$scope.query = function(){
		CommandApi.queryText({database : $routeParams.database, language : 'sql', text : $scope.queryText, limit : $scope.limit},function(data){
			$scope.headers = Object.keys(data.result[0]);
			$scope.results = data.result;
		});
	}
	$scope.openRecord = function(doc){
		$location.path("/database/" + $scope.database.dbName + "/browse/" + doc["@rid"].replace('#',''));
	}
}]);
