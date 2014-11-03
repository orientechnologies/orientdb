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
        var editor = new EpicEditor({
          container: elem[0],
          basePath: "bower_components/epiceditor/epiceditor",
          button: {
            fullscreen: false,
            bar: "auto"
          }
        }).load(function () {

          });


        if (value) {
          editor.getElement('editor').body.innerHTML = value;
        }
        editor.preview();
        unregister();
      }
    }

  }
});
