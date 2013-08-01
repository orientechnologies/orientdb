var  DocController = angular.module('document.controller',[]);
DocController.controller("DocumentEditController",['$scope','$routeParams','$location','$modal','$dialog','$q','DocumentApi','Database','Notification',function($scope,$routeParams,$location,$modal,$dialog,$q,DocumentApi,Database,Notification){

	var database = $routeParams.database;
	var rid = $routeParams.rid;
	$scope.fixed = Database.header;
	$scope.canSave = true;
	$scope.canDelete = true;
	$scope.canCreate = true;
	$scope.canAdd = true;

	// Toggle modal
	$scope.showModal = function(rid) {
		modalScope = $scope.$new(true);	
		modalScope.db = database;
		modalScope.rid = rid;
		var modalPromise = $modal({template: '/views/document/modalEdit.html', persist: true, show: false, backdrop: 'static',modalClass : 'editEdge',scope: modalScope});
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
	if(!$scope.doc){
		$scope.reload();
	}else {
		$scope.headers = Database.getPropertyFromDoc($scope.doc);
		$scope.isGraph = Database.isGraph($scope.doc['@class']);
		$scope.incomings = Database.getEdge($scope.doc,'in');
		$scope.outgoings = Database.getEdge($scope.doc,'out');
		$scope.outgoings = $scope.outgoings.concat((Database.getLink($scope.doc)));
	}
	$scope.save = function(){
		if(!$scope.isNew){
			DocumentApi.updateDocument(database,rid,$scope.doc,function(data){
				Notification.push({content : data});
				$scope.reload();
			});
		}else {
			DocumentApi.createDocument(database,$scope.doc['@rid'],$scope.doc,function(data){
				Notification.push({content : JSON.stringify(data)});
				$location.path('/database/'+database + '/browse/edit/' + data['@rid'].replace('#',''));
			});
		}
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
	$scope.deleteField = function(name){
		Utilities.confirm($scope,$dialog,{
			title : 'Warning!',
			body : 'You are removing field '+ name + ' from Vertex ' + $scope.doc['@rid'] + '. Are you sure?',
			success : function() {
				delete $scope.doc[name];
				$scope.save();
			}
		});
	}
	$scope.deleteLink = function(group,rid) {
		var index = $scope.doc[group].indexOf(rid);
		$scope.doc[group].splice(index,1);
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

	$scope.types = Database.getSupportedTypes();
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
	$scope.addField = function(name,type){
		if(name){
			$scope.doc[name] = null;
			var types = $scope.doc['@fieldTypes'];
			if(types){
				types = types + ',' + name + '=' + Database.getMappingFor(type);
			}else {
				types = name + '=' + Database.getMappingFor(type);	
			}
			
			$scope.doc['@fieldTypes'] = types;
			$scope.headers.push(name);
		}
	}
	$scope.deleteField = function(name){
		delete $scope.doc[name];
		var idx = $scope.headers.indexOf(name);
		$scope.headers.splice(idx,1);
	}
	$scope.reload();
}]);
DocController.controller("EditController",['$scope','$routeParams','$location','DocumentApi','Database','Notification',function($scope,$routeParams,$location,DocumentApi,Database,Notification){

	var database = $routeParams.database;
	var rid = $routeParams.rid;
	$scope.doc = DocumentApi.get({ database : database , document : rid},function(){

		$scope.template = Database.isGraph($scope.doc['@class']) ? '/views/database/editVertex.html' : '/views/database/editDocument.html'
	}, function(error){
		Notification.push({content : JSON.stringify(error)});
		$location.path('/404');
	});

	
}]);
DocController.controller("CreateController",['$scope','$routeParams','$location','DocumentApi','Database','Notification',function($scope,$routeParams,$location,DocumentApi,Database,Notification){
	
	var database = $routeParams.database;
	var clazz = $routeParams.clazz
	$scope.fixed = Database.header;
	$scope.doc = DocumentApi.createNewDoc(clazz);
	$scope.headers = Database.getPropertyFromDoc($scope.doc);
	$scope.isNew = true;
	$scope.template = Database.isGraph(clazz) ? '/views/database/editVertex.html' : '/views/database/editDocument.html'
	
}]);