var  GrapgController = angular.module('vertex.controller',['ui.bootstrap']);
GrapgController.controller("VertexEditController",['$scope','$routeParams','$location','$modal','$q','$dialog','DocumentApi','Database','CommandApi','Notification',function($scope,$routeParams,$location,$modal,$q,$dialog,DocumentApi,Database,CommandApi,Notification){

	var database = $routeParams.database;
	var rid = $routeParams.rid;
	$scope.fixed = Database.header;
	$scope.canSave = true;
	$scope.canDelete = true;
	$scope.canCreate = true;
	$scope.popover = {
		title : 'Add edge'
	}

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
	$scope.showModalConnection = function(label) {
		var modalScope = $scope.$new(true);	
		modalScope.db = database;
		modalScope.originRid = rid;
		modalScope.container = $scope;	
		modalScope.label = label
		var modalPromise = $modal({template: '/views/vertex/modalConnection.html', persist: true, show: false, backdrop: 'static',scope: modalScope,modalClass : 'createEdge'});
		$q.when(modalPromise).then(function(modalEl) {
			modalEl.modal('show');
		});
	}
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
	$scope.delete = function(){

		var recordID = $scope.doc['@rid']
		Utilities.confirm($scope,$dialog,{
			title : 'Warning!',
			body : 'You are removing vertex '+ recordID + '. Are you sure?',
			success : function() {
				var command = "DELETE Vertex " + recordID;
				CommandApi.queryText({database : database, language : 'sql', text : command},function(data){
					var clazz = $scope.doc['@class'];
					$location.path('/database/'+database + '/browse/' + 'select * from ' + clazz);
				});
			}
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
			body : 'You are removing edge '+ edge + '. Are you sure?',
			success : function() {
				var edgeDoc = DocumentApi.get({ database : database , document : edge},function(){
					var command =""
					if(Database.isEdge(edgeDoc['@class'])){
						command = "DELETE EDGE " + edge;	
					}
					else {
						if(group.contains('in_')){
							command = "DELETE EDGE FROM " + edge + " TO " + rid;
						}else {
							command = "DELETE EDGE FROM " + rid + " TO " + edge;
						}
					}
					CommandApi.queryText({database : database, language : 'sql', text : command},function(data){
						$scope.reload();
					});
				}, function(error){
					Notification.push({content : JSON.stringify(error)});
					$location.path('/404');
				});
				
			}
		});
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
GrapgController.controller("VertexPopoverLabelController",['$scope','$routeParams','$location','DocumentApi','Database','Notification',function($scope,$routeParams,$location,DocumentApi,Database,Notification){

	$scope.init = function(where){
		$scope.where = where;
		$scope.labels = Database.getClazzEdge();
	}
	$scope.addEdgeLabel = function(){
		var name = "";
		if($scope.where  == "outgoings"){
			name = "out_".concat($scope.popover.name);
		}
		else {
			name = "in_".concat($scope.popover.name);
		}
		if($scope[$scope.where].indexOf(name)==-1)
			$scope[$scope.where].push(name);
		delete $scope.popover.name;
	}
	
}]);

GrapgController.controller("VertexModalBrowseController",['$scope','$routeParams','$location','Database','CommandApi',function($scope,$routeParams,$location,Database,CommandApi){

	$scope.database = Database;
	$scope.limit = 20;
	$scope.queries = new Array;
	$scope.added = new Array;
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
		CommandApi.queryText({database : $routeParams.database, language : 'sql', text : $scope.queryText, limit : $scope.limit},function(data){
			if(data.result){
				$scope.headers = Database.getPropertyTableFromResults(data.result);
				$scope.results = data.result;
			}
			if($scope.queries.indexOf($scope.queryText)==-1)
				$scope.queries.push($scope.queryText);
		});
	}
	$scope.select = function(result){
		var index = $scope.added.indexOf(result['@rid']);
		if(index==-1){
			$scope.added.push(result['@rid']);
		}else {
			$scope.added.splice(index,1);
		}
	}
	$scope.createEdges = function(){

		var command;
		if($scope.label.contains('in_')){
			command = "CREATE EDGE " + $scope.label.replace("in_","") + " FROM [" + $scope.added + "]" + " TO " + $scope.originRid;	
		}else {
			command = "CREATE EDGE " + $scope.label.replace("out_","") + " FROM " + $scope.originRid + " TO [" + $scope.added + "]";	
		}
		CommandApi.queryText({database : $routeParams.database, language : 'sql', text : command},function(data){
			$scope.added = new Array;		
			$scope.container.reload();
		});
		
	}
}]);