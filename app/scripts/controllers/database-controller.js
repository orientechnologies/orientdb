var dbModule = angular.module('database.controller',['database.services']);
dbModule.controller("BrowseController",['$scope','$routeParams','$location','Database','CommandApi','Spinner','ngTableParams',function($scope,$routeParams,$location,Database,CommandApi,Spinner,ngTableParams){

	$scope.database = Database;
	$scope.limit = 20;
	$scope.queries = new Array;
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
        Spinner.loading = true;
		CommandApi.queryText({database : $routeParams.database, language : 'sql', text : $scope.queryText, limit : $scope.limit},function(data){
			if(data.result){
				$scope.headers = Database.getPropertyTableFromResults(data.result);
                $scope.resultTotal = data.result;
				$scope.results = data.result.slice(0,10);
                $scope.currentPage = 1;
                $scope.countPage = 10;
                $scope.numberOfPage = new Array( Math.round(data.result.length / 10));

			}
			if($scope.queries.indexOf($scope.queryText)==-1)
				$scope.queries.push($scope.queryText);
            Spinner.loading = false;
		});
	}
    $scope.switchPage= function(index){
        if(index != $scope.currentPage){
        $scope.currentPage = index;
        $scope.results = $scope.resultTotal.slice(
            (index - 1) * $scope.countPage,
            index * $scope.countPage
        );
        }
    }
	$scope.openRecord = function(doc){
		$location.path("/database/" + $scope.database.getName() + "/browse/edit/" + doc["@rid"].replace('#',''));
	}

	if($routeParams.query){
		$scope.queryText = $routeParams.query;
		$scope.query();
	}
}]);

