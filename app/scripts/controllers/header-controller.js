angular.module('header.controller',['database.services']).controller("HeaderController",['$scope','$routeParams','$http','$location','Database',function($scope,$routeParams,$http,$location,Database){
	$scope.database = Database;
	$scope.$watch(Database.getName,function(data){
		if(data!=null){
			$scope.menus = [
			{ name : "browse", link : '#/database/'+ data +'/browse'},
			{ name : "schema", link : '#/database/'+ data +'/schema'},
			{ name : "users" , link : '#/database/'+ data +'/users'}
			];
		}
	});
	
	$scope.logout = function(){
		Database.disconnect();
		$location.path("/");
	}
}]);