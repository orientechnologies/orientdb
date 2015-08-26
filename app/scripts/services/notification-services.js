var notification = angular.module('notification.services', []);

notification.factory('Notification', function ($timeout, $rootScope) {


  var notiService = {
    notifications: new Array,
    errors: new Array,
    warnings: new Array,

    push: function (notification) {


      this.notifications.splice(0, this.notifications.length);
      this.errors.splice(0, this.errors.length);
      this.warnings.splice(0, this.warnings.length);


      if (notification.error) {
        if (typeof notification.content != 'string') {
          notification.content = notification.content.errors[0].content;
        }
        this.errors.push(notification);
      } else if (notification.warning) {
        this.warnings.push(notification);
      }
      else {
        this.notifications.push(notification);
      }
      var self = this;
      if (!notification.error) {
        self.stopTimer();
        self.startTimer();
      }
    },
    startTimer: function () {
      var self = this;
      self.timePromise = $timeout(function () {
        self.clear();
      }, 4000)
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
      this.warnings.splice(0, this.warnings.length);
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
