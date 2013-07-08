var  DocController = angular.module('document.controller',[]);
DocController.controller("DocumentEditController",['$scope','$routeParams','$location','DocumentApi','Database','Notification',function($scope,$routeParams,$location,DocumentApi,Database,Notification){

	var database = $routeParams.database;
	var rid = $routeParams.rid;
	$scope.fixed = Database.header;
	
	$scope.reload = function(){

		$scope.doc = DocumentApi.get({ database : database , document : rid},function(){
			$scope.headers = Object.keys($scope.doc).splice(4,Number.MAX_VALUE);
			if($scope.headers[$scope.headers.length -1] == '@fieldTypes'){
				$scope.headers.pop();
			}
		}, function(error){
			Notification.push({content : JSON.stringify(error)});
			$location.path('/404');
		});
	}
	$scope.reload();
	$scope.save = function(){
		DocumentApi.updateDocument(database,rid,$scope.doc,function(data){
			Notification.push({content : data});
			$scope.reload();
		});
		
	}
	$scope.create = function(){
		$location.path('/database/'+database + '/browse/create/' + $scope.doc['@class']);
	}
}]);
DocController.controller("DocumentCreateController",['$scope','$routeParams','$location','DocumentApi','Database','Notification',function($scope,$routeParams,$location,DocumentApi,Database,Notification){

	var database = $routeParams.database;
	var clazz = $routeParams.clazz
	$scope.fixed = Database.header;
	$scope.doc = DocumentApi.createNewDoc(clazz);
	$scope.headers = Object.keys($scope.doc).splice(4,Number.MAX_VALUE);
	if($scope.headers[$scope.headers.length -1] == '@fieldTypes'){
		$scope.headers.pop();
	}
	$scope.save = function(){
		DocumentApi.createDocument(database,$scope.doc['@rid'],$scope.doc,function(data){
			Notification.push({content : JSON.stringify(data)});
			$location.path('/database/'+database + '/browse/edit/' + data['@rid'].replace('#',''));
		});
		
	}
}]);