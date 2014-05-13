var notification = angular.module('notification.services', []);

notification.factory('Notification', function () {

    return {
        notifications: new Array,
        errors: new Array,

        push: function (notification) {
            this.notifications.splice(0, this.notifications.length);
            this.errors.splice(0, this.errors.length);

            if (notification.error) {
                this.errors.push(notification);
            } else {
                this.notifications.push(notification);
            }
        },
        clear: function () {
            this.notifications.splice(0, this.notifications.length);
            this.errors.splice(0, this.errors.length);
        }

    }
});