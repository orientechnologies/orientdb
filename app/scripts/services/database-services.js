var database = angular.module('database.services', ['ngResource']);

DatabaseResolve = {
	current : function (Database,$q,$route,$location){
		var deferred = $q.defer();

		Database.refreshMetadata($route.current.params.database,function(){
			deferred.resolve();
		})
		return deferred.promise;
	},
	delay : function ($q , $timeout){
		var delay = $q.defer();
		$timeout(delay.resolve, 1000);
		return delay.promise;
	}
}  
database.factory('Database', function(DatabaseApi,localStorageService){
	var current = {
		name : null,
		username : null,
		metadata : null
	}
	return { 

		header : ["@rid","@version","@class"],

		exclude : ["@type","@fieldTypes"],

		getMetadata : function() {
			if(current.metadata ==null){
				var tmp = localStorageService.get("CurrentDB");
				if(tmp !=null) current = tmp;
			}
			return current.metadata;
		},
		setMetadata : function(metadata){
			current.metadata = metadata;
		},
		currentUser : function() {
			return current !=null ? current.username : null;
		},
		setCurrentUser : function(user){
			current.username = user;
		},
		setName : function(name){
			current.name = name;
		},
		getName : function() {
			return current !=null ? current.name : current;
		},
		refreshMetadata : function(database,callback){
			var currentDb = DatabaseApi.get({database : database},function(){ 
				current.name = database;
				current.username = currentDb.currentUser;
				current.metadata = currentDb;
				localStorageService.add("CurrentDB",current);
				callback();
			});
		},
		isConnected : function(){
			return current.username !=null;
		},
		connect : function (database,username,password,callback){
			var self = this;
			DatabaseApi.connect(database,username,password,function(){
				callback();
			});
		},
		disconnect : function(){
			DatabaseApi.disconnect(function(){
				
			});
			delete current.name;
			delete current.username;
			delete current.metadata;
			localStorageService.clearAll();
			localStorageService.cookie.clearAll();
			document.cookie = "";
		},
		findType : function(clazz,value,field){
			var metadata = this.getMetadata();
			if(metadata==null) return "STRING";
			var classes =  metadata['classes'];
			for (var entry in classes){
				if(clazz.toUpperCase() == classes[entry].name.toUpperCase()){
					var props = classes[entry]['properties'];
					for (var f in props) {
						if(field == props[f].name){
							return props[f].type;
						}	
					};
				}
			}
			return "STRING"
		},
		getDateFormat : function(){
			return "yyyy-mm-dd"
		},
		getDateTimeFormat : function(){
			return "yyyy-mm-dd HH:mm:ss";
		},
		listField : function(clazz){
			var metadata = this.getMetadata();
			var classes =  metadata['classes'];
			var fields = new Array
			for (var entry in classes){
				var defaultCluster = classes[entry]['defaultCluster'];
				if(clazz.toUpperCase() == classes[entry].name.toUpperCase()){
					var props = classes[entry]['properties'];
					for (var f in props) {
						fields.push(props[f].name);
					};
					break;
				}				
			}
			return fields;
		},
		classFromCluster : function(cluster){
			var metadata = this.getMetadata();
			var classes =  metadata['classes'];
			var clazz ;
			for (var entry in classes){
				var defaultCluster = classes[entry]['defaultCluster'];
				if(cluster == defaultCluster){
					clazz =  classes[entry].name;
					break;
				}				
			}
			return clazz;
		},
		getSuperClazz : function(clazz){
			var metadata = this.getMetadata();
			var classes =  metadata['classes'];
			var clazz ;
			for (var entry in classes){
				var name = classes[entry]['name'];
				if(clazz == name){
					clazz =  classes[entry].superClass;
					break;
				}				
			}
			return clazz;
		},
		isGraph : function(clazz){
			var sup=clazz; 
			var iterator = clazz;
			while( (iterator = this.getSuperClazz(iterator)) != "") {
			 	sup = iterator;
			}
			return sup == 'V' || sup == 'E';
		},
		/**
 		* Creates a new Array from a document with property name.
 		*
 		* @param {doc} OrientDB Document.
 		* @return {Array} Property name Array.
 		*/
		getPropertyFromDoc : function(doc){
			var isGraph = this.isGraph(doc['@class']);
			var fixedHeader = this.header.concat(this.exclude);
			var all = Object.keys(doc).filter(function(element,index,array){
				if(isGraph){
					return (fixedHeader.indexOf(element) == -1 && (!element.startsWith("in") && !element.startsWith("out")));
				}else {
					return (fixedHeader.indexOf(element) == -1);
				}
			});
			return all;
		},
		getEdge : function(doc,direction){
			
			var all = Object.keys(doc).filter(function(element,index,array){
				return element.startsWith(direction);
			});
			return all;
		},
		/**
 		* Creates a new Array with property name from a result set of documents.
 		*
 		* @param {results} OrientDB result set.
 		* @return {Array} Property name Array.
 		*/
		getPropertyTableFromResults : function(results){
			var self = this;
			var headers = new Array;
			results.forEach(function(element, index, array){
				var tmp = Object.keys(element);
				if(headers.length==0){
					headers = headers.concat(tmp);
				}else {
					var tmp2 = tmp.filter(function(element,index,array){
						return headers.indexOf(element) == -1;
					});
					headers = headers.concat(tmp2);
				}
			});
			var all = headers.filter(function(element,index,array){
				return self.exclude.indexOf(element) == -1;
			});
			return all;
		}

	};
}) ;

database.factory('DatabaseApi', function($http,$resource){

	var resource = $resource('/api/database/:database');
	resource.listDatabases = function(callback) {
		$http.get('/api/listDatabases').success(callback);
	}
	resource.connect = function(database,username,password,callback) {
		$http.defaults.headers.common['Authorization'] = 'Basic ' + Base64.encode(username + ':' + password);
		$http.get('/api/connect/' + database).success(callback);
	}
	resource.disconnect = function(database,username,password,callback) {		
		$http.get('/api/disconnect').success(function(){
			$http.defaults.headers.common['Authorization'] = null;
			callback();
		});
	}
	return resource;
}) ;
database.factory('CommandApi', function($http,$resource,Notification){

	var resource = $resource('/api/command/:database');

	resource.queryText = function(params,callback){
		var startTime = new Date().getTime();
		$http.post('/api/command/' + params.database + "/" + params.language + "/" + params.text + "/" + params.limit).success(function(data){
			var time = ((new Date().getTime() - startTime) / 1000);
			var noti = "Query executed in " + time + " sec. Returned " + data.result.length + " record(s)"; 
			Notification.push({content : noti});
			callback(data);
		}).error(function(data){
			Notification.push({content : data});
		});
	}
	return resource;
}) ;
database.factory('DocumentApi', function($http,$resource,Database){

	var resource = $resource('/api/document/:database/:document');
	resource.updateDocument = function (database,rid,doc,callback){
		$http.put('/api/document/' + database + "/" + rid.replace('#',''),doc).success(callback).error(callback);		
	}
	resource.createDocument = function (database,rid,doc,callback){
		$http.post('/api/document/' + database + "/" + rid.replace('#',''),doc).success(callback).error(callback);		
	}
	resource.createNewDoc = function(clazz){
		var r = new resource
		var fields = Database.listField(clazz);
		r['@class'] = clazz;
		r['@version'] = 0;
		r['@rid'] = '#-1:-1';
		for (var i = 0; i < fields.length; i++) {
			r[fields[i]] = null;
		};
		return r;
	}
	return resource;
}) ;
database.factory('FunctionApi', function($http,$resource){

	
	var resource = $resource('/api/tournaments/:id');
	return resource;
}) ;