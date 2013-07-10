var schemaModule = angular.module('schema.controller',['database.services']);
schemaModule.controller("SchemaController",['$scope','$routeParams','$location','Database','CommandApi',function($scope,$routeParams,$location,Database,CommandApi){

	$scope.database = Database;

	$scope.list = $scope.database.listClasses();
	
	$scope.headers = ['name','superClass','alias','abstract','clusters','defaultCluster','records'];

	

	$scope.setClass = function(clazz){
		$scope.classClicked = clazz;
	}  
	$scope.openClass = function(clazz){
		$location.path("/database/" + $scope.database.getName() + "/browse/editclass/" + clazz.name);
	}
}]);
schemaModule.controller("ClassEditController",['$scope','$routeParams','$location','Database','CommandApi',function($scope,$routeParams,$location,Database,CommandApi){


	var clazz = $routeParams.clazz;


	$scope.classClickedHeaders = ['name','type','linkedType','linkedClass','mandatory','readonly','notNull','min','max'];

	$scope.property = Database.listPropertiesForClass(clazz);
	

}]);