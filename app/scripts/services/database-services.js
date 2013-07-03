var database = angular.module('database.services', ['ngResource']);
database.factory('Database', function(){
	return { current : { }};
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
	return resource;
}) ;
database.factory('CommandApi', function($http,$resource){

	var resource = $resource('/api/command/:database');

	resource.queryText = function(params,callback){
		$http.post('/api/command/' + params.database + "/" + params.language + "/" + params.text + "/" + params.limit).success(callback);
	}
	return resource;
}) ;
database.factory('FunctionApi', function($http,$resource){

	
	var resource = $resource('/api/tournaments/:id');
	return resource;
}) ;