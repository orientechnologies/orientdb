var  DocController = angular.module('document.controller',[]);
DocController.controller("DocumentEditController",['$scope','$routeParams','$location','DocumentApi','Database','Notification',function($scope,$routeParams,$location,DocumentApi,Database,Notification){

	var database = $routeParams.database;
	var rid = $routeParams.rid;
	$scope.fixed = Database.header;
	
	$scope.reload = function(){

		$scope.doc = DocumentApi.get({ database : database , document : rid},function(){
			$scope.headers = Database.getPropertyFromDoc($scope.doc);
			$scope.incomings = Database.getEdge($scope.doc,'in');
			$scope.outgoings = Database.getEdge($scope.doc,'out');
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
	$scope.filterArray = function(arr) {
		if(arr instanceof Array){
			return arr;
		}else {
			var newArr = new Array;
			newArr.push(arr);
			return newArr;
		}

	}
	$scope.navigate = function(rid){
		$location.path('/database/'+database + '/browse/edit/' + rid.replace('#',''));
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
	$scope.headers = Database.getPropertyFromDoc($scope.doc);
	$scope.save = function(){
		DocumentApi.createDocument(database,$scope.doc['@rid'],$scope.doc,function(data){
			Notification.push({content : JSON.stringify(data)});
			$location.path('/database/'+database + '/browse/edit/' + data['@rid'].replace('#',''));
		});
		
	}
}]);