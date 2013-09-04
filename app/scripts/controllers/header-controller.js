angular.module('header.controller',['database.services']).controller("HeaderController",['$scope','$routeParams','$http','$location','Database',function($scope,$routeParams,$http,$location,Database){
	$scope.database = Database;
    $scope.selectedMenu = null;
    $scope.menus = [];
	$scope.$watch(Database.getName,function(data){
        $scope.menus = [
            { name : "browse", link : '#/database/'+ data +'/browse'},
            { name : "schema", link : '#/database/'+ data +'/schema'},
            { name : "users" , link : '#/database/'+ data +'/users'},
            { name: "functions", link: '#/database/' + data + '/functions'}
            { name: "info", link: '#/database/' + data + '/info'}

        ];
        if(data!=null){
            $scope.setSelected();
		}
	});

    $scope.setSelected = function(){

        $scope.menus.forEach(function(element,index,array){
            var find = $location.path().indexOf("/" + element.name);
            if(find!=-1){
                $scope.selectedMenu = element;
            }

        });
    }
    $scope.getClass = function(menu){
          return menu == $scope.selectedMenu ? 'active' : '';
    }
    $scope.$on('$routeChangeSuccess', function(scope, next, current){
        $scope.setSelected();
    });
	$scope.logout = function(){
		Database.disconnect(function(){
			$scope.menus = [];
			$location.path("/");	
		});
	}
}]);