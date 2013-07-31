var breadcrumb = angular.module('breadcrumb.services', []);

breadcrumb.factory('Breadcrumb',function($rootScope,Database){

	var bread = {
		breadcrumbs : [],

		push : function(bread){
			this.breadcrumbs.push(bread);
		},
		clear : function(){
			this.breadcrumbs.length = 0;
		}
	};
	$rootScope.$watch(Database.getName,function(data){
		if(data){
			bread.push({link : "#/database/"+data+'/browse', name : "Browse"});
		}
	});
	return bread;
});