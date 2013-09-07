var breadcrumb = angular.module('breadcrumb.services', []);

breadcrumb.factory('Breadcrumb',function($rootScope,Database,$location){

	var bread = {
		breadcrumbs : [],

		push : function(bread){
			this.breadcrumbs.push(bread);
		},
		clear : function(){
			this.breadcrumbs.length = 0;
		}
	};
    $rootScope.$on('$routeChangeSuccess', function(event,current, prev) {

        var path =  '/database/' + Database.getName() + "/";
        var arr =   $location.path().replace(path,"");
        var array = new Array;
        arr = arr.split("/");
        bread.clear();
        arr.forEach(function(val,idx,arrs){

            var link = '#/database/' + Database.getName() + "/";
            for(i=0;i<=idx;i++){
                link += arrs[i] + (i==idx ? "" : "/");
            }
            bread.push({ link : link , name : val.toUpperCase()});
        });


    });
	$rootScope.$watch(Database.getName,function(data){

	});
	return bread;
});