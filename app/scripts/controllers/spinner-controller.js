/**
 * Created with JetBrains WebStorm.
 * User: erisa
 * Date: 26/08/13
 * Time: 13.54
 * To change this template use File | Settings | File Templates.
 */

angular.module('spinner.controller',['spinner.services']).controller("SpinnerController",['$scope','$routeParams','$http','$location','Spinner',function($scope,$routeParams,$http,$location,Spinner){
    $scope.sp = Spinner;

/*    $scope.$on('$routeChangeStart', function(scope, next, current){
        Spinner.loading = true;
    });

    $scope.$on('$routeChangeSuccess', function(scope, next, current){
        Spinner.loading = false;
    });*/
}]);
