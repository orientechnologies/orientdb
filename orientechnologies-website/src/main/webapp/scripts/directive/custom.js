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

angular.module('webappApp').directive('vueEditor', function ($timeout, $compile, $http, $typeahead) {
  return {
    require: '^ngModel',
    scope: {
      preview: "=?preview",
      placeholder: "=?placeholder",
      onSend: '&'
    },
    templateUrl: 'views/vueditor.html',
    controller: function ($scope) {
      $scope.preview = $scope.preview || true
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
        scope.$parent.$watch('actors', function (val) {
          if (val) {
            scope.actors = val.map(function (a) {
              return {label: a.name};
            });
          }
        })


        if (!editor) {
          scope.placeholder = scope.placeholder || 'Leave a comment'
          var defaultVal = scope.preview ? 'No description' : '';
          editor = new Vue({
            el: elem[0],
            data: {
              input: value || defaultVal
            },
            filters: {
              marked: marked
            },
            methods: {
              send: function (e) {
                if ((e.keyCode == 13) && (!e.ctrlKey)) {
                  if (scope.onSend && editor.$data.input && editor.$data.input.length > 0) {
                    scope.onSend()
                  }
                }
              }
            }
          })

          scope.$parent.$watch('sending', function (val) {
            scope.sending = val;
          })
          editor.$watch('$data.input', function (newVal, oldval) {
            ngModel.$setViewValue(newVal);

          });
        } else {
          var defaultVal = scope.preview ? 'No description' : '';
          editor.$data.input = value || defaultVal
        }
        scope.selectActor = function (item) {
          var selected = item.label;
          editor.$data.input += " ";
          return selected;
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
    controller: function ($scope) {

    },
    templateUrl: 'views/avatar.html'
  }
});


angular.module('scroll', []).directive('whenScrolled', function () {
  return function (scope, elm, attr) {
    var raw = elm[0];

    elm.bind('scroll', function () {
      if (raw.scrollTop == 0) {
        scope.$apply(attr.whenScrolled);
      }
    });
  };
});

(function ($, undefined) {
  $.fn.getCursorPosition = function () {
    var el = $(this).get(0);
    var pos = 0;
    if ('selectionStart' in el) {
      pos = el.selectionStart;
    } else if ('selection' in document) {
      el.focus();
      var Sel = document.selection.createRange();
      var SelLength = document.selection.createRange().text.length;
      Sel.moveStart('character', -el.value.length);
      pos = Sel.text.length - SelLength;
    }
    return pos;
  }
})(jQuery);
