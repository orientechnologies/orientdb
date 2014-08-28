var notification = angular.module('notification.services', []);

notification.factory('Notification', function ($timeout) {

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
            var self = this;
            if (notification.autoHide) {
                $timeout(function () {
                    self.clear();
                }, 3000)
            }
        },
        clear: function () {
            this.notifications.splice(0, this.notifications.length);
            this.errors.splice(0, this.errors.length);
        }

    }
});