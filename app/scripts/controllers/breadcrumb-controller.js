angular.module('breadcrumb.controller',['breadcrumb.services']).controller("BreadcrumbController",['$scope','$routeParams','$http','$location','Breadcrumb',function($scope,$routeParams,$http,$location,Breadcrumb){
	$scope.breads = Breadcrumb.breadcrumbs;
}]);