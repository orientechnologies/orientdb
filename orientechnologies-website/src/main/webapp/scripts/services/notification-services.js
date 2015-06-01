angular.module('webappApp').factory("Notification", function ($rootScope) {


    return {
        success: function (data) {
            var jacked = humane.create({baseCls: 'humane-jackedup', addnCls: 'humane-jackedup-success'})
            jacked.log(data);
        }
    };
});