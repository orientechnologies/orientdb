
angular.module('notification.controller',['notification.services']).controller("NotificationController",['$scope','Notification',function($scope,Notification){
	$scope.alerts = Notification.notifications;
}]);