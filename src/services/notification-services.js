let notification = angular.module('notification.services', []);
import noty from 'noty';

notification.factory('Notification', function ($timeout, $rootScope, $sanitize) {


  var notiService = {
    notifications: new Array,
    errors: new Array,
    warnings: new Array,

    push: function (notification) {


      this.notifications.splice(0, this.notifications.length);
      this.errors.splice(0, this.errors.length);
      this.warnings.splice(0, this.warnings.length);


      var n;

      if (this.current) {
        this.current.close();
      }
      if (notification.error) {
        if (typeof notification.content != 'string') {
          notification.content = notification.content.errors[0].content;
        }
        n = noty({text: _.escape(notification.content), layout: 'bottom', type: 'error', theme: 'relax'});
      } else if (notification.warning) {
        n = noty({text: notification.content, layout: 'bottom', type: 'warning', theme: 'relax'});
      } else {
        n = noty({text: notification.content, layout: 'bottom', type: 'success', theme: 'relax'});
      }
      this.current = n;
      $timeout(function () {
        if (n && !(n.options.type === 'error'))
          n.close();
      }, 4000);
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

export default notification.name;
