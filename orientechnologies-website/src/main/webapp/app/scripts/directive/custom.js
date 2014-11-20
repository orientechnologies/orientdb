'use strict';

angular.module('webappApp').directive('mkeditor', function ($timeout) {
  return {
    require: '^ngModel',
    scope: {
      readOnly: "=readOnly"
    },
    link: function (scope, elem, attrs, ngModel) {


      var editor;
      scope.$watch(function () {
        return ngModel.$modelValue;
      }, initialize);

      function initialize(value) {
        if (value) {
          ngModel.$setViewValue(value);

        }
        if (!editor) {
          $(elem).markdown({
            autofocus: false,
            savable: false,
            onShow: function (e) {

              e.setContent(value);
              //e.showPreview();
              editor = e;
            },
            onChange: function (e) {
              ngModel.$setViewValue(e.getContent());
            }
          })

        } else {
          editor.setContent(value);
        }

      }
    }
  }
});
angular.module('webappApp').directive('avatar', function ($timeout) {
  return {
    restrict: 'E',
    scope: {
      user: "=user",
      dim: "=dim",
      name: "=name"
    },
    controller  : function($scope){

    },
    templateUrl: 'views/avatar.html'
  }
});
