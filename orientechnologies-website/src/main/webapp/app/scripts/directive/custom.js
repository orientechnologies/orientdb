'use strict';

angular.module('webappApp').directive('mkeditor', function ($timeout) {
  return {
    require: '^ngModel',
    link: function (scope, elem, attrs, ngModel) {


      var unregister = scope.$watch(function () {
        return ngModel.$modelValue;
      }, initialize);

      function initialize(value) {
        if (value) {
          ngModel.$setViewValue(value);
        }

        $(elem[0]).markdown({
          autofocus: false,
          savable: false,
          onShow: function (e) {
            e.setContent(value);
          }
        })


        unregister();
      }
    }

  }
});
