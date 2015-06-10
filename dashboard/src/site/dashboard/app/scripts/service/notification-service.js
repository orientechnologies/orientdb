var noti = angular.module('humane.services', []);


noti.factory('Humane', function () {


  return {
    success: function (text) {

      var jacked = humane.create({baseCls: 'humane-jackedup', addnCls: 'humane-jackedup-success'})
      jacked.log(text);
    },
    error: function (text) {
      var jacked = humane.create({baseCls: 'humane-jackedup', addnCls: 'humane-jackedup-error'})
      jacked.log(text)
    }
  }
})

