angular.module("webappApp").factory("Notification", function($rootScope) {
  return {
    success: function(data, timeout) {
      var timeout = timeout || 3000;
      var jacked = humane.create({
        baseCls: "humane-jackedup",
        addnCls: "humane-jackedup-success",
        timeout: timeout
      });
      jacked.log(data);
    },
    error: function(data, timeout) {
      var timeout = timeout || 3000;
      var jacked = humane.create({
        baseCls: "humane-jackedup",
        addnCls: "humane-jackedup-error",
        timeout: timeout
      });
      jacked.log(data);
    }
  };
});
