var schemaModule = angular.module('schema.controller',['database.services']);
schemaModule.controller("SchemaController",['$scope','$routeParams','$location','Database','CommandApi',function($scope,$routeParams,$location,Database,CommandApi){

	$scope.database = Database;

	$scope.list = $scope.database.listClasses();
	
	$scope.headers = ['name','superClass','alias','abstract','clusters','defaultCluster','records']
	$scope.classClickedHeaders = ['name','type','linkedType','abstract','clusters','defaultCluster','records']

	$scope.classClicked ;

	$scope.setClass = function(clazz){
		$scope.classClicked = clazz;
	}  

}]);