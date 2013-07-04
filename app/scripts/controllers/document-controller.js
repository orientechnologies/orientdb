angular.module('document.controller',[]).controller("DocumentController",['$scope','$routeParams','DocumentApi',function($scope,$routeParams,DocumentApi){

	var database = $routeParams.database;
	var rid = $routeParams.rid;
	$scope.doc = DocumentApi.get({ database : database , document : rid},function(){
		$scope.headers = Object.keys($scope.doc);
	});
	
	$scope.save = function(){
		DocumentApi.updateDocument(database,rid,$scope.doc,function(data){
			console.log(data);
		});
	}
}]);