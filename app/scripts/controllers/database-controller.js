var dbModule = angular.module('database.controller',['database.services']);
dbModule.controller("DatabaseController",['$scope','$routeParams','$location','Database','CommandApi',function($scope,$routeParams,$location,Database,CommandApi){

	$scope.database = Database;
	$scope.action = $routeParams.action;
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
        theme : 'ambiance',
        mode: 'text/x-sql',
        extraKeys: {
    		"Ctrl-Enter": function(instance) { $scope.query() }
  		}
    };
	$scope.query = function(){
		CommandApi.queryText({database : $routeParams.database, language : 'sql', text : $scope.queryText, limit : $scope.limit},function(data){
			var resultHeader = data.result.length > 0 ? Object.keys(data.result[0]) : [];
			$scope.headers = $scope.parseHeader(resultHeader); 
			$scope.results = data.result;
		});
	}
	$scope.parseHeader = function (headers){
		return Database.header.concat( headers.splice(4,Number.MAX_VALUE));
	}
	$scope.openRecord = function(doc){
		$location.path("/database/" + $scope.database.getName() + "/browse/edit/" + doc["@rid"].replace('#',''));
	}
}]);
