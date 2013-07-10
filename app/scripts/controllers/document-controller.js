var  DocController = angular.module('document.controller',[]);
DocController.controller("DocumentEditController",['$scope','$routeParams','$location','$modal','$q','DocumentApi','Database','Notification',function($scope,$routeParams,$location,$modal,$q,DocumentApi,Database,Notification){

	var database = $routeParams.database;
	var rid = $routeParams.rid;
	$scope.fixed = Database.header;
	
	
 
	// Toggle modal
	$scope.showModal = function() {
	  var modalPromise = $modal({template: '/views/database/modalEdit.html', persist: true, show: false, backdrop: 'static'});
	  $q.when(modalPromise).then(function(modalEl) {
	    modalEl.modal('show');
	  });
	};
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
DocController.controller("DocumentModalController",['$scope','$routeParams','$location','DocumentApi','Database','Notification',function($scope,$routeParams,$location,DocumentApi,Database,Notification){

	var db = 'tinkerpop';
	var rid = '#11:0';
	$scope.reload = function(){
		$scope.doc = DocumentApi.get({ database : db , document : rid},function(){
			$scope.headers = Database.getPropertyFromDoc($scope.doc);
		}, function(error){
			Notification.push({content : JSON.stringify(error)});
			$location.path('/404');
		});
	}
	$scope.reload();
	$scope.save = function(){
		DocumentApi.updateDocument(db,rid,$scope.doc,function(data){
			Notification.push({content : data});
			$scope.reload();
		});
		
	}
}]);