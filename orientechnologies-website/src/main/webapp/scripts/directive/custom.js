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

angular.module('webappApp').directive('autofocus', ['$timeout', function ($timeout) {
  return {
    restrict: 'A',
    link: function ($scope, $element) {
      $timeout(function () {
        $element[0].focus();
      });
    }
  }
}]);

angular.module('webappApp').directive('dropzone', ['AccessToken', function (AccessToken) {
  return {
    restrict: 'A',
    scope: {
      url: "=?url",
    },
    link: function ($scope, $element, $attrs) {

      var token = AccessToken.get();
      $scope.$watch("url", function (val) {
          if (val) {
            $element.dropzone({
              url: val,
              maxFilesize: MAX_ATTACHMENT,
              paramName: "file",
              maxThumbnailFilesize: 5,
              init: function () {


                this.on('success', function (file, json) {
                  this.removeFile(file);
                  $scope.$emit('file-uploaded', json);
                });

                this.on('addedfile', function (file) {

                });

                this.on('drop', function (file) {

                });
                this.on('error', function (file, err, errCode) {
                  $scope.$emit('file-uploaded-error', err, errCode);
                  this.removeFile(file);
                })

              },
              headers: {
                "X-AUTH-TOKEN": token
              }
            });
          }
        }
      )

    }
  }
}])
;
angular.module('webappApp').directive('vueEditor', function ($timeout, $compile, $http, $typeahead) {
  return {
    require: '^ngModel',
    scope: {
      preview: "=?preview",
      carriage: "=?carriage",
      placeholder: "=?placeholder",
      nullValue: "=?nullValue",
      onSend: '&'
    },
    templateUrl: 'views/vueditor.html',
    controller: function ($scope) {
      if ($scope.preview == undefined) {
        $scope.preview = true
      }
      if ($scope.carriage == undefined) {
        $scope.carriage = false;
      }
    },
    link: function (scope, elem, attrs, ngModel) {
      var editor;


      scope.$watch(function () {
        return ngModel.$modelValue;
      }, initialize);

      function initialize(value) {

        ngModel.$setViewValue(value);

        if (!editor) {
          var showing = false;

          scope.$parent.$watch('actors', function (val) {
            if (val) {
              scope.actors = val;

              var text = elem.children()[0];
              $(elem.children()[0]).suggest('@', {
                data: scope.actors,
                map: function (user) {
                  return {
                    value: user.name,
                    text: '<strong>' + user.name + '</strong>'
                  }
                },
                onshow: function (e) {
                  showing = true;
                },
                onselect: function (e) {
                  editor.$data.input = $(text).val()
                  showing = false;
                },
                onhide: function () {
                  showing = false;
                }

              })
            }
          })

          scope.placeholder = scope.placeholder || 'Leave a comment'
          var defaultVal = scope.preview ? 'No description' : '';
          defaultVal = scope.nullValue ? scope.nullValue : defaultVal;
          var elementArea = elem[0];

          $(elementArea).focus();
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


                if (!scope.carriage && (e.keyCode == 13) && (!e.ctrlKey && !e.shiftKey) && !showing) {
                  e.preventDefault();
                  if (scope.onSend && editor.$data.input && editor.$data.input.length > 0) {
                    scope.onSend();
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
          defaultVal = scope.nullValue ? scope.nullValue : defaultVal;
          editor.$data.input = value || defaultVal
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

        // console.log($scope);
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


/**
 * the HTML5 autofocus property can be finicky when it comes to dynamically loaded
 * templates and such with AngularJS. Use this simple directive to
 * tame this beast once and for all.
 *
 * Usage:
 * <input type="text" autofocus>
 */
angular.module('utils.autofocus', [])

  .directive('autofocus', ['$timeout', function ($timeout) {
    return {
      restrict: 'A',
      link: function ($scope, $element) {
        $timeout(function () {
          $element[0].focus();
        }, 200);
      }
    }
  }]);

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
