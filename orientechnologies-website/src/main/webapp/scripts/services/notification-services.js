angular.module('webappApp').factory("Notification", function ($rootScope) {


  return {
    success: function (data) {
      var jacked = humane.create({baseCls: 'humane-jackedup', addnCls: 'humane-jackedup-success', timeout: 3000})
      jacked.log(data);
    },
    error: function (data) {
      var jacked = humane.create({baseCls: 'humane-jackedup', addnCls: 'humane-jackedup-error', timeout: 3000})
      jacked.log(data);
    }
  };
});
