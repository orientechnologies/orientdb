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
schemaModule.controller("ClassEditController",['$scope','$routeParams','$location','Database','CommandApi','$modal','$q','$dialog','$route',function($scope,$routeParams,$location,Database,CommandApi,$modal,$q,$dialog,$route){


	var clazz = $routeParams.clazz;


	$scope.database = Database;


	$scope.listClasses = $scope.database.listNameOfClasses();


	$scope.limit = 20;
	$scope.queries = new Array;

	$scope.classClickedHeaders = ['name','type','linkedType','linkedClass','mandatory','readonly','notNull','min','max',''];

	$scope.property = Database.listPropertiesForClass(clazz);
	
	$scope.propertyNames = new Array;
	for(inn in $scope.property){
			$scope.propertyNames.push($scope.property[inn]['name'])
	}
	// console.log($scope.propertyNames)

	$scope.indexes = Database.listIndexesForClass(clazz);
	// for(zz in $scope.indexes)
	// 	console.log(zz)
	
	$scope.queryText = ""
	$scope.modificati = new Array;
	$scope.listTypes = ['BINARY','BOOLEAN','EMBEDDED','EMBEDDEDLIST','EMBEDDEDMAP','EMBEDDEDSET','DECIMAL','FLOAT','DATE','DATETIME','DOUBLE','INTEGER','LINK','LINKLIST','LINKMAP','LINKSET','LONG','SHORT','STRING'];


	
	$scope.modificato = function(result,prop){
		var key = result['name'];
		// console.log(result[prop])
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
	$scope.newIndex = function() {
		modalScope = $scope.$new(true);	
		modalScope.db = database;
		modalScope.classInject = clazz;
		modalScope.parentScope = $scope;
		modalScope.propertiesName = $scope.propertyNames ;
		// modalScope.rid = rid;
		var modalPromise = $modal({template: '/views/database/newIndex.html', scope: modalScope});
		$q.when(modalPromise).then(function(modalEl) {
			modalEl.modal('show');
		});
	};	
	$scope.newProperty = function() {
		modalScope = $scope.$new(true);	
		modalScope.db = database;
		modalScope.classInject = clazz;
		modalScope.parentScope = $scope;
		modalScope.propertiesName = $scope.propertyNames ;
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
	$scope.dropIndex = function(nameIndex){
		
		
		
		


		Utilities.confirm($scope,$dialog,{

			title : 'Warning!',
			body : 'You are dropping index '+ nameIndex + '. Are you sure?',
			success : function() {
				var sql = 'DROP INDEX ' + nameIndex;

				CommandApi.queryText({database : $routeParams.database, language : 'sql', text : sql, limit : $scope.limit},function(data){

				});
				var index = $scope.indexes.indexOf($scope.indexes[nameIndex])
				$scope.indexes.splice(index,1)
				$scope.indexes.splice()

			}
			
		});

	}

	$scope.dropProperty = function(result,elementName){
		// console.log(result);
		Utilities.confirm($scope,$dialog,{

			title : 'Warning!',
			body : 'You are dropping property  '+ elementName + '. Are you sure?',
			success : function() {
				var sql = 'DROP PROPERTY ' + clazz + '.' + elementName;

				for(entry in $scope.property ){
					if($scope.property[entry]['name'] == elementName){
						// console.log($scope.property[entry])
						var index = $scope.property.indexOf($scope.property[entry])
						$scope.property.splice(index,1)
					}
				}
				CommandApi.queryText({database : $routeParams.database, language : 'sql', text : sql, limit : $scope.limit},function(data){

				});

			}
			
		});
	}
	$scope.checkDisable = function(res,entry){
		if(res[entry] == null || res[entry] == undefined || res[entry] == ""){
			// console.log('false');
			return false;
		}
		// console.log('true')
		return true;
	}
	$scope.checkTypeEdit = function(res){

		var occupato =  $scope.checkDisable(res,'linkedClass');
		if(occupato){
		res['linkedType'] = null;
			return true;
		}
		if(res['type'] == 'EMBEDDEDLIST' || res['type'] =='EMBEDDEDSET' || res['type'] =='EMBEDDEDMAP' ){
			return false;
		}
		res['linkedType'] = null;
		return true;
	}
	$scope.checkClassEdit = function(res){

		var occupatoType =  $scope.checkDisable(res,'linkedType');
		if(occupatoType){
		res['linkedClass'] = null;
			return true;
		}
		
		if(res['type'] == 'LINKLIST' || res['type'] =='LINKSET' || res['type'] =='LINKMAP' || res['type'] =='EMBEDDED' || res['type'] == 'EMBEDDEDLIST' || res['type'] =='EMBEDDEDSET' || res['type'] =='EMBEDDEDMAP'){
			return false;
		}
		
		res['linkedClass'] = null;
		return true;


	}
	$scope.refreshPage = function(){
		
		$route.reload();
	}
		



}]);


schemaModule.controller("IndexController",['$scope','$routeParams','$location','Database','CommandApi','$modal','$q',function($scope,$routeParams,$location,Database,CommandApi,$modal,$q){



	$scope.listTypeIndex = [ 'DICTIONARY', 'FULLTEXT', 'UNIQUE', 'NOTUNIQUE' ];
	$scope.newIndex = 	{"name": "", "type": "", "fields": "" }
	$scope.namesProp = $scope.propertiesName;
	$scope.prop2add = new Array;

	// for(zz in $scope.namesProp )
	// console.log($scope.namesProp[zz])

	$scope.addedField = function(nameField){
		var index = $scope.prop2add.indexOf(nameField);
		console.log(index)
		if(index==-1){
			$scope.prop2add.push(nameField)
		}
		else{
			$scope.prop2add.splice(index,1)
		}
		console.log($scope.prop2add);
	}

	$scope.saveNewIndex = function(){
		
		if($scope.newIndex['name']==undefined || $scope.newIndex['name']=="" || $scope.newIndex['name']==null)
			return;
		if($scope.newIndex['type']==undefined || $scope.newIndex['type']=="" || $scope.newIndex['type']==null)
			return;
		if($scope.prop2add.length == 0)
			return;
		var proppps = '';
		var first = true
		for(entry in $scope.prop2add){
			if(first){
				proppps = proppps + $scope.prop2add[entry] ;
				first=!first
			}
			else{
				proppps = proppps + ',' + $scope.prop2add[entry];
			}
			console.log(proppps);

		}

		var sql = 'CREATE INDEX '+$scope.newIndex['name'] + ' ON ' + $scope.classInject + ' ( ' + proppps+ ' ) ' +   $scope.newIndex['type'];
		CommandApi.queryText({database : $routeParams.database, language : 'sql', text : sql, limit : $scope.limit},function(data){
		$scope.hide();
		$scope.parentScope.refreshPage();
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
			// console.log('false');
			return false;
		}
		// console.log('true')
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
