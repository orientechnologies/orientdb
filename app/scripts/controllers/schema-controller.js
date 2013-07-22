var schemaModule = angular.module('schema.controller',['database.services']);
schemaModule.controller("SchemaController",['$scope','$routeParams','$location','Database','CommandApi',function($scope,$routeParams,$location,Database,CommandApi){

	$scope.database = Database;
	$scope.listClasses = $scope.database.listClasses();

	$scope.headers = ['name','superClass','alias','abstract','clusters','defaultCluster','records'];



	$scope.setClass = function(clazz){
		$scope.classClicked = clazz;
	}  
	$scope.openClass = function(clazz){
		$location.path("/database/" + $scope.database.getName() + "/browse/editclass/" + clazz.name);
	}
}]);
schemaModule.controller("ClassEditController",['$scope','$routeParams','$location','Database','CommandApi','$modal','$q','$dialog',function($scope,$routeParams,$location,Database,CommandApi,$modal,$q,$dialog){


	var clazz = $routeParams.clazz;


	$scope.database = Database;


	$scope.listClasses = $scope.database.listNameOfClasses();


	$scope.limit = 20;
	$scope.queries = new Array;

	$scope.classClickedHeaders = ['name','type','linkedType','linkedClass','mandatory','readonly','notNull','min','max',''];

	$scope.property = Database.listPropertiesForClass(clazz);

	$scope.indexes = Database.listIndexesForClass(clazz);

	console.log($scope.indexes);
	$scope.queryText = ""
	$scope.modificati = new Array;
	$scope.listTypes = ['BINARY','BOOLEAN','EMBEDDED','EMBEDDEDLIST','EMBEDDEDMAP','EMBEDDEDSET','DECIMAL','FLOAT','DATE','DATETIME','DOUBLE','INTEGER','LINK','LINKLIST','LINKMAP','LINKSET','LONG','SHORT','STRING'];


	
	$scope.modificato = function(result,prop){
		var key = result['name'];
		console.log(result[prop])
		if($scope.modificati[result['name']] == undefined){
			$scope.modificati[result['name']] = new Array(prop);
		}

		else {

			var elem= $scope.modificati[result['name']] 
			var already = false;
			for(i in elem){
				if(prop == elem[i]){
					already=true

				}
			}
			if(already == false){

				elem.push(prop);
			}

		}

	}
	$scope.newProperty = function() {
		modalScope = $scope.$new(true);	
		modalScope.db = database;
		modalScope.classInject = clazz;
		modalScope.parentScope = $scope;
		// modalScope.rid = rid;
		var modalPromise = $modal({template: '/views/database/newProperty.html', scope: modalScope});
		$q.when(modalPromise).then(function(modalEl) {
			modalEl.modal('show');
		});
	};	
	$scope.addProperties = function(prop){
		$scope.property.push(prop);
	}
	$scope.saveProperty = function(properties){

		for (result in properties ){

		//il nome da andare a cercare nella lista dei modificati
		var keyName = properties[result]['name'];
		//l'array da modificare
		var arrayToUpdate = $scope.modificati[keyName];

		if(arrayToUpdate != undefined){

			for(i in arrayToUpdate){

				var prop = arrayToUpdate[i];
				var newValue = properties[result][prop]!= '' ? properties[result][prop] : null; 
				var sql = 'ALTER PROPERTY ' + clazz + '.' + keyName +' ' +prop+ ' ' +newValue;
			// console.log(sql);
			CommandApi.queryText({database : $routeParams.database, language : 'sql', text : sql, limit : $scope.limit},function(data){

			});
		}
	}
}
		//clear
		$scope.modificati = new Array;
	}


	$scope.dropProperty = function(result,elementName){
		console.log(result);
		Utilities.confirm($scope,$dialog,{

			title : 'Warning!',
			body : 'You are dropping property  '+ elementName + '. Are you sure?',
			success : function() {
				var sql = 'DROP PROPERTY ' + clazz + '.' + elementName;

				for(entry in $scope.property ){
					if($scope.property[entry]['name'] == elementName){
						console.log($scope.property[entry])
						var index = $scope.property.indexOf($scope.property[entry])
						$scope.property.splice(index,1)
					}
				}
				CommandApi.queryText({database : $routeParams.database, language : 'sql', text : sql, limit : $scope.limit},function(data){

				});

			}
			
		});
	}	

}]);





schemaModule.controller("PropertyController",['$scope','$routeParams','$location','Database','CommandApi','$modal','$q',function($scope,$routeParams,$location,Database,CommandApi,$modal,$q){


	$scope.property = {"name": "","type": "" ,"linkedType": "","linkedClass": "" , "mandatory": "false","readonly": "false","notNull": "false" ,"min": null,"max": null}

	$scope.listTypes = ['BINARY','BOOLEAN','EMBEDDED','EMBEDDEDLIST','EMBEDDEDMAP','EMBEDDEDSET','DECIMAL','FLOAT','DATE','DATETIME','DOUBLE','INTEGER','LINK','LINKLIST','LINKMAp','LINKSET','LONG','SHORT','STRING'];

	$scope.database = Database;

	$scope.listClasses = $scope.database.listNameOfClasses();


	$scope.salvaProperty = function(){


		var prop= $scope.property;

		var propName = $scope.property['name'];

		var propType = $scope.property['type'];

		if(propName == undefined || propType == undefined)
			return;

		var linkedType = prop['linkedType'];
		var linkedClass = prop['linkedClass'];
		var sql = 'CREATE PROPERTY ' +$scope.classInject + '.'+propName + ' ' +  propType + ' '+  linkedType+ ' ' +linkedClass ;
		CommandApi.queryText({database : $routeParams.database, language : 'sql', text : sql, limit : $scope.limit},function(data){

		});

		var i = 1;
		for(entry in prop){
			var sql = 'ALTER PROPERTY ' +$scope.classInject + '.' + propName +' ' +entry+ ' ' +prop[entry];			
			CommandApi.queryText({database : $routeParams.database, language : 'sql', text : sql, limit : $scope.limit},function(data){
				i++;
				if(i == 5){
					$scope.database.refreshMetadata($routeParams.database,function(){
						$scope.parentScope.addProperties(prop);
					});
					$scope.hide();
				}
			});
		}

	}

	$scope.checkDisable = function(entry){
		if($scope.property[entry] == null || $scope.property[entry] == undefined || $scope.property[entry] == ""){
			console.log('false');
			return false;
		}
		console.log('true')
		return true;
	}
	$scope.checkDisableLinkedType = function(entry){

		var occupato =  $scope.checkDisable('linkedClass');
		if(occupato){
$scope.property['linkedType'] = null;
			return true;
		}
		if($scope.property['type'] == 'EMBEDDEDLIST' || $scope.property['type'] =='EMBEDDEDSET' || $scope.property['type'] =='EMBEDDEDMAP' ){
			return false;
		}
		$scope.property['linkedType'] = null;
		return true;
	}
	$scope.checkDisableLinkedClass = function(entry){

		var occupatoType =  $scope.checkDisable('linkedType');
		if(occupatoType){
		$scope.property['linkedClass'] = null;
			return true;
		}
		
		if($scope.property['type'] == 'LINKLIST' || $scope.property['type'] =='LINKSET' || $scope.property['type'] =='LINKMAP' || $scope.property['type'] =='EMBEDDED' || $scope.property['type'] == 'EMBEDDEDLIST' || $scope.property['type'] =='EMBEDDEDSET' || $scope.property['type'] =='EMBEDDEDMAP'){
			return false;
		}
		
		$scope.property['linkedClass'] = null;
		return true;
	}


}]);
