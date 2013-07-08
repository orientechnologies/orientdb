var notification = angular.module('notification.services', []);

notification.factory('Notification',function(){

	return {
		notifications : new Array,

		push : function(notification){
			this.notifications.splice(0, this.notifications.length);
			this.notifications.push(notification);
		}

	}
});