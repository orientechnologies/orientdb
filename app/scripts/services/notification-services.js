var notification = angular.module('notification.services', []);

notification.factory('Notification', function ($timeout, $rootScope) {


    var notiService = {
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
            self.stopTimer();
            self.startTimer();
        },
        startTimer: function () {
            var self = this;
            self.timePromise = $timeout(function () {
                self.clear();
            }, 3000)
        },
        stopTimer: function () {
            var self = this;
            if (self.timePromise) {
                $timeout.cancel(self.timePromise);
            }
        },
        clear: function () {
            this.notifications.splice(0, this.notifications.length);
            this.errors.splice(0, this.errors.length);
        }

    }

    $rootScope.$on('alert:hover', function () {
        notiService.stopTimer();
    })
    $rootScope.$on('alert:out', function () {
        notiService.startTimer();
    })
    return notiService;
});