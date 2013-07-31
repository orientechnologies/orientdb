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

		listTypes : ['BINARY','BOOLEAN','EMBEDDED','EMBEDDEDLIST','EMBEDDEDMAP','EMBEDDEDSET','DECIMAL','FLOAT','DATE','DATETIME','DOUBLE','INTEGER','LINK','LINKLIST','LINKMAP','LINKSET','LONG','SHORT','STRING'],

		mapping : { 'BINARY' : 'b','DATE' : 'a','DATETIME' : 't'},
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
		getMappingFor : function(type){
			return this.mapping[type];
		},
		getMappingForKey : function(key){
			var self = this;
			var type = "STRING";
			Object.keys(this.mapping).forEach(function(elem,index,array){
				if(self.mapping[elem] == key){
					type = elem;
				}
			});
			
			return type;
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
		getSupportedTypes : function(){
			return this.listTypes;
		},
		refreshMetadata : function(database,callback){
			var currentDb = DatabaseApi.get({database : database},function(){ 
				current.name = database;
				current.username = currentDb.currentUser;
				current.metadata = currentDb;
				localStorageService.add("CurrentDB",current);
				if(callback)
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
			var type = "STRING";
			if(typeof value === 'number'){
     		   type = "INTEGER";
    		}
			return type;
		},
		getDateFormat : function(){
			return "yyyy-mm-dd"
		},
		getDateTimeFormat : function(){
			return "yyyy-mm-dd HH:mm:ss";
		},
		getFieldType : function(clazz,field){
			var metadata = this.getMetadata();
			var classes =  metadata['classes'];
			var type = undefined;
			classes.forEach(function(element,index,array){
				if(element.name.toUpperCase() == clazz.toUpperCase()){
					if(element['properties']){
						element['properties'].forEach(function(element,index,array){
							if(element.name == field){
								type = element.type;
							}
						});
					}
				}
			});	
			return type;
		},
		isLink : function(type){
			return type == "LINKSET" || type == "LINK"
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
		listPropertiesForClass : function(clazz){
			var metadata = this.getMetadata();
			var classes =  metadata['classes'];
			var fields = new Array
			for (var entry in classes){
				var defaultCluster = classes[entry]['properties'];
				if(clazz.toUpperCase() == classes[entry].name.toUpperCase()){
					var props = classes[entry]['properties'];
					for (var f in props) {
						fields.push(props[f]);
					};
					break;
				}				
			}
			return fields;
		},
		listPropertyForClass : function(clazz,field){
			var metadata = this.getMetadata();
			var classes =  metadata['classes'];
			var property = undefined;
			classes.forEach(function(element,index,array){
				if(element.name.toUpperCase() == clazz.toUpperCase()){
					if(element['properties']){
						element['properties'].forEach(function(element,index,array){
							if(element.name == field){
								property = element;
							}
						});
					}
				}
			});	
			return property;
		},
		listIndexesForClass : function(clazz){
			var metadata = this.getMetadata();
			var classes =  metadata['classes'];
			var fields = new Array
			for (var entry in classes){
				var defaultCluster = classes[entry]['indexes'];
				if(clazz.toUpperCase() == classes[entry].name.toUpperCase()){
					var props = classes[entry]['indexes'];
					for (var f in props) {
						
				console.log(props[f])
						fields.push(props[f]);
					};
					break;
				}				
			}
			return fields;
		},
		listClasses : function(){
			var metadata = this.getMetadata();
			var classes =  metadata['classes'];
			var fields = new Array
			for (var entry in classes){
				var claq = classes[entry].name
				fields.push(classes[entry])					
			}
			return fields;
		},
		listNameOfClasses : function(){
			var metadata = this.getMetadata();
			var classes =  metadata['classes'];
			var fields = new Array
			for (var entry in classes){
				var claq = classes[entry]['name']
				fields.push(claq)					
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
			var clazzReturn = "" ;
			for (var entry in classes){
				var name = classes[entry]['name'];
				if(clazz == name){
					clazzReturn =  classes[entry].superClass;
					break;
				}				
			}
			return clazzReturn;
		},
		isGraph : function(clazz){
			var sup=clazz; 
			var iterator = clazz;
			while( (iterator = this.getSuperClazz(iterator)) != "") {
			 	sup = iterator;
			}
			return sup == 'V' || sup == 'E';
		},
		isVertex : function(clazz){
			var sup=clazz; 
			var iterator = clazz;
			while( (iterator = this.getSuperClazz(iterator)) != "") {
			 	sup = iterator;
			}
			return sup == 'V';
		},
		isEdge : function(clazz){
			var sup=clazz; 
			var iterator = clazz;
			while( (iterator = this.getSuperClazz(iterator)) != "") {
			 	sup = iterator;
			}
			return sup == 'E';
		},
		getClazzEdge : function(){
			var metadata = this.getMetadata();
			var classes =  metadata['classes'];
			var clazzes = new Array ;
			for (var entry in classes){
				var name = classes[entry]['name'];
				if(this.isEdge(name)){
					clazzes.push(name);
				} 			
			}
			return clazzes;
		},
		/**
 		* Creates a new Array from a document with property name.
 		*
 		* @param {doc} OrientDB Document.
 		* @return {Array} Property name Array.
 		*/
		getPropertyFromDoc : function(doc){
			var c = doc['@class'];
			var isGraph = this.isGraph(c);
			var fixedHeader = this.header.concat(this.exclude);
			var self = this;
			var all = Object.keys(doc).filter(function(element,index,array){
				if(isGraph){
					return (fixedHeader.indexOf(element) == -1 && (!element.startsWith("in") && !element.startsWith("out"))&& !self.isLink(type));
				}else {
					var type = self.getFieldType(c,element);
					return (fixedHeader.indexOf(element) == -1 && !self.isLink(type));
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

		getLink : function(doc){
			var self = this;
			var all = Object.keys(doc).filter(function(element,index,array){
				var type = self.getFieldType(doc['@class'],element);
				return self.isLink(type);
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
	resource.createDatabase = function(name,type,stype,username,password,callback) {
		$http.defaults.headers.common['Authorization'] = 'Basic ' + Base64.encode(username + ':' + password);
		$http.post('/api/database/' + name + "/" + stype + "/"+ type).success(function(data){
			$http.defaults.headers.common['Authorization'] = null;
			callback(data);
		});	
	}
	resource.disconnect = function(database,username,password,callback) {	
		$http.post('/api/disconnect').success(function(){
			$http.defaults.headers.common['Authorization'] = null;
			callback();
		});
	}
	return resource;
}) ;
database.factory('CommandApi', function($http,$resource,Notification){

	var resource = $resource('/api/command/:database');

	resource.queryText = function(params,callback,error){
		var startTime = new Date().getTime();
		var limit = params.limit || 20;
		//rid,type,version,class,attribSameRow,indent:2,dateAsLong,shalow,graph
		var text = '/api/command/' + params.database + "/" + params.language + "/-/" + limit + '?format=rid,type,version,class,shallow,graph' ;
		var query = params.text ;
		$http.post(text,query).success(function(data){
			var time = ((new Date().getTime() - startTime) / 1000);
			var records = data.result ? data.result.length : "";
			var noti = "Query executed in " + time + " sec. Returned " + records + " record(s)"; 
			Notification.push({content : noti});
			callback(data);
		}).error(function(data){
			Notification.push({content : data});
			if(error) error(data);
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