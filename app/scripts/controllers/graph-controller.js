var  GrapgController = angular.module('vertex.controller',['ui.bootstrap']);
GrapgController.controller("VertexEditController",['$scope','$routeParams','$location','$modal','$q','$dialog','DocumentApi','Database','CommandApi','Notification',function($scope,$routeParams,$location,$modal,$q,$dialog,DocumentApi,Database,CommandApi,Notification){

	var database = $routeParams.database;
	var rid = $routeParams.rid;
	$scope.fixed = Database.header;
	$scope.canSave = true;
	$scope.canDelete = true;
	$scope.canCreate = true;
	

	// Toggle modal
	$scope.showModal = function(rid) {
		var modalScope = $scope.$new(true);	
		modalScope.db = database;
		modalScope.rid = rid;
		var modalPromise = $modal({template: '/views/database/modalEdit.html', persist: true, show: false, backdrop: 'static',scope: modalScope});
		$q.when(modalPromise).then(function(modalEl) {
			modalEl.modal('show');
		});
	};
	$scope.reload = function(){

		$scope.doc = DocumentApi.get({ database : database , document : rid},function(){
			$scope.headers = Database.getPropertyFromDoc($scope.doc);
			$scope.isGraph = Database.isGraph($scope.doc['@class']);
			$scope.incomings = Database.getEdge($scope.doc,'in');
			$scope.outgoings = Database.getEdge($scope.doc,'out');
			$scope.outgoings = $scope.outgoings.concat((Database.getLink($scope.doc)));
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
	$scope.deleteLink = function(group,edge) {
		
		Utilities.confirm($scope,$dialog,{
			title : 'Warning!',
			body : 'You are removing edge '+ rid + '. Are you sure?',
			success : function() {
				var command =""
				if($scope.doc[group] instanceof Array){
					command = "DELETE EDGE " + edge;	
				}else {
					command = "DELETE EDGE FROM " + rid + " TO " + edge;
				}
				CommandApi.queryText({database : database, language : 'sql', text : command},function(data){
					$scope.reload();
				});
			}
		});
		
		
		// var index = $scope.doc[group].indexOf(rid);
		// $scope.doc[group].splice(index,1);
	}
	$scope.addEdgeLabel = function(){
		$scope.outgoings.push("New Relationship") ;
	}
	$scope.navigate = function(rid){
		$location.path('/database/'+database + '/browse/edit/' + rid.replace('#',''));
	}
	$scope.create = function(){
		$location.path('/database/'+database + '/browse/create/' + $scope.doc['@class']);
	}
}]);
GrapgController.controller("VertexCreateController",['$scope','$routeParams','$location','DocumentApi','Database','Notification',function($scope,$routeParams,$location,DocumentApi,Database,Notification){


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
GrapgController.controller("VertexModalController",['$scope','$routeParams','$location','DocumentApi','Database','Notification',function($scope,$routeParams,$location,DocumentApi,Database,Notification){


	$scope.reload = function(){
		$scope.doc = DocumentApi.get({ database : $scope.db , document : $scope.rid},function(){
			$scope.headers = Database.getPropertyFromDoc($scope.doc);
		}, function(error){
			Notification.push({content : JSON.stringify(error)});
			$location.path('/404');
		});
	}
	$scope.save = function(){
		DocumentApi.updateDocument($scope.db,$scope.rid,$scope.doc,function(data){
			Notification.push({content : data});
		});
		
	}
	$scope.reload();
}]);