import angular from 'angular';
var Widget = angular.module('rendering', []);


import '../views/widget/string.html';
import '../views/widget/short.html';
import '../views/widget/embedded.html';
import '../views/widget/embeddedmap.html';
import '../views/widget/boolean.html';
import '../views/widget/binary.html';
import '../views/widget/byte.html';
import '../views/widget/date.html';
import '../views/widget/datetime.html';
import '../views/widget/decimal.html';
import '../views/widget/double.html';
import '../views/widget/float.html';
import '../views/widget/embeddedset.html';
import '../views/widget/embeddedlist.html';
import '../views/widget/integer.html';
import '../views/widget/long.html';
import '../views/widget/link.html';

import '../vendor/moment-jdateformatparser';

Widget.directive('docwidget', ["$compile", "$http", "Database", "CommandApi", "DocumentApi", "$timeout", function ($compile, $http, Database, CommandApi, DocumentApi, $timeout) {


  var compileForm = function (response, scope, element, attrs) {
    var formScope = scope.$new(true);
    formScope.doc = scope.doc;
    formScope.database = scope.database;
    formScope.deleteField = scope.deleteField;
    formScope.options = new Array;
    formScope.types = Database.getSupportedTypes();
    formScope.fieldTypes = new Array;
    formScope.selectedHeader = {}
    formScope.editorOptions = {
      lineWrapping: true,
      lineNumbers: true,
      readOnly: false,
      mode: 'application/json',
      onChange: function (_editor) {

      }
    };
    formScope.onLoadEditor = function (_editor) {
    }
    formScope.inSchema = function (header) {
      if (scope.doc['@class']) {
        var property = Database.listPropertyForClass(scope.doc['@class'], header);
        return property;
      }
    }
    formScope.isSelected = function (name, type) {
      return type == formScope.getType(name);
    }
    formScope.getTemplate = function (header) {


      if (formScope.doc['@class']) {

        let type = formScope.selectedHeader[header];
        if (!type) {
          type = findType(formScope, header);
        }
        if (type) {
          return 'views/widget/' + type.toLowerCase() + '.html';
        }
        else {
          return 'views/widget/string.html';
        }
      }
    }
    formScope.getType = function (header) {
      if (formScope.doc['@class']) {
        return findType(formScope, header);
      } else {
        return "STRING";
      }
    }
    formScope.requiredString = function (header) {
      return formScope.isRequired(header) ? "*" : "";
    }
    formScope.isRequired = function (header) {

      var property = Database.listPropertyForClass(formScope.doc['@class'], header);
      if (property) {
        return property.mandatory;
      } else {
        return false;
      }
    }
    formScope.findAll = function (header) {
      var property = Database.listPropertyForClass(formScope.doc['@class'], header);
      if (!formScope.options[header]) {
        CommandApi.getAll(scope.database, property.linkedClass, function (data) {
          formScope.options[header] = data.result;
        });
      }
    }
    formScope.changeType = function (name) {


      let type = formScope.selectedHeader[name];

      var idx = formScope.headers.indexOf(name);
      formScope.headers.splice(idx, 1);

      var types = formScope.doc['@fieldTypes'];
      if (types) {
        types = types + ',' + name + '=' + Database.getMappingFor(type);
      } else {
        types = name + '=' + Database.getMappingFor(type);
      }
      formScope.doc['@fieldTypes'] = types;
      formScope.fieldTypes[name] = formScope.getTemplate(name);
      formScope.headers.push(name);
    }
    formScope.$parent.$watch('docForm.$valid', function (validity) {
      scope.docValid = validity;
    });


    formScope.handleFile = function (header, files) {

      var reader = new FileReader();
      reader.onload = function (event) {
        object = {};
        object.filename = files[0].name;
        object.data = event.target.result;
        var blobInput = [event.target.result];
        var blob = new Blob(blobInput);
        formScope.doc[header] = "$file";
        DocumentApi.uploadFileDocument(formScope.database, formScope.doc, blob, files[0].name);
      }
      reader.readAsDataURL(files[0]);
    }
    scope.$on('fieldAdded', function (event, field) {
      formScope.selectedHeader[field] = formScope.getType(field);
      formScope.fieldTypes[field] = formScope.getTemplate(field);
    });
    scope.$parent.$watch("headers", function (data) {
      if (data) {
        data.forEach(function (elem, idx, array) {
          formScope.fieldTypes[elem] = formScope.getTemplate(elem);
          formScope.selectedHeader[elem] = formScope.getType(elem);
        });

        formScope.headers = data;
      }

    });


    var el = angular.element($compile(response)(formScope));
    element.empty();
    element.append(el);
  }
  var findType = function (scope, name) {
    var type = null;
    var property = Database.listPropertyForClass(scope.doc['@class'], name);


    var guessType = function (value) {
      var value = scope.doc[name];
      var type = null;
      if (typeof value === 'number') {
        type = "INTEGER";
      } else if (typeof value === 'boolean') {
        type = "BOOLEAN";
      } else if (value instanceof Array) {
        return "EMBEDDED";
      } else if (value instanceof Object) {
        return "EMBEDDED";
      }
      return type;
    }
    if (!property) {
      var fieldTypes = scope.doc['@fieldTypes'];
      var type = Database.findTypeFromFieldTipes(scope.doc, name);
      if (!type) {
        type = guessType(scope.doc[name])

      }
      property = new Object;
      property.name = name;
    } else {
      type = property.type;
    }
    return type != null ? type : "STRING";
  }
  var linker = function (scope, element, attrs) {

    var url = attrs.docwidget ? attrs.docwidget : "views/widget/form.html"

    let tpl = `
          <div ng-include="'${url}'"></div>
        `
    compileForm(tpl, scope, element, attrs);
    // $http.get(url).then(function (response) {
    //   compileForm(response, scope, element, attrs);
    // });
  }
  return {
    // A = attribute, E = Element, C = Class and M = HTML Comment
    restrict: 'A',
    //The link function is responsible for registering DOM listeners as well as updating the DOM.
    link: linker
  }
}]);

Widget.directive('jsontext', function () {

  var formatter = (function () {

    function repeat(s, count) {
      return new Array(count + 1).join(s);
    }

    function formatJson(json, indentChars) {
      var i = 0,
        il = 0,
        tab = (typeof indentChars !== "undefined") ? indentChars : "    ",
        newJson = "",
        indentLevel = 0,
        inString = false,
        currentChar = null;

      for (i = 0, il = json.length; i < il; i += 1) {
        currentChar = json.charAt(i);

        switch (currentChar) {
          case '{':
          case '[':
            if (!inString) {
              newJson += currentChar + "\r\n" + repeat(tab, indentLevel + 1);
              indentLevel += 1;
            } else {
              newJson += currentChar;
            }
            break;
          case '}':
          case ']':
            if (!inString) {
              indentLevel -= 1;
              newJson += "\r\n" + repeat(tab, indentLevel) + currentChar;
            } else {
              newJson += currentChar;
            }
            break;
          case ',':
            if (!inString) {
              newJson += ",\r\n" + repeat(tab, indentLevel);
            } else {
              newJson += currentChar;
            }
            break;
          case ':':
            if (!inString) {
              newJson += ": ";
            } else {
              newJson += currentChar;
            }
            break;
          case ' ':
          case "\n":
          case "\t":
            if (inString) {
              newJson += currentChar;
            }
            break;
          case '"':
            if (i > 0 && json.charAt(i - 1) !== '\\') {
              inString = !inString;
            }
            newJson += currentChar;
            break;
          default:
            newJson += currentChar;
            break;
        }
      }
      return newJson;
    }

    return {"formatJson": formatJson};

  }());

  return {
    restrict: 'A',
    require: 'ngModel',
    link: function (scope, element, attr, ngModel) {
      function into(input) {
        if (input) {
          var obj = JSON.parse(input);
          return obj;
        }
        return input;
      }

      function out(data) {
        if (data) {
          var string = data instanceof Object ? JSON.stringify(data) : data;
          return formatter.formatJson(string);
        }
      }

      ngModel.$parsers.push(into);
      ngModel.$formatters.push(out);

    }
  };
});

Widget.directive('chartjs', function () {


  return {
    restrict: 'A',
    link: function (scope, element, attr) {

      var data = scope.$eval(attr.chartjs);
      //new Chart(element.get(0).getContext("2d")).Pie(data, {segmentShowStroke: false});

    }
  };
});
Widget.directive('orientdate', ['Database', function (Database) {


  return {
    priority: 1001,
    restrict: 'A',
    require: 'ngModel',
    link: function (scope, element, attr, ngModel) {

      function into(input) {

        var values = Database.getMetadata()['config']['values'];
        var formatter = undefined;
        values.forEach(function (val, idx, array) {
          if (val.name == 'dateFormat') {
            formatter = val.value;
          }
        });

        var form = input;
        if (input) {
          var form = moment(input).format(formatter.toUpperCase());
        }

        return form;
      }

      function out(data) {
        var values = Database.getMetadata()['config']['values'];
        var formatter = undefined;
        values.forEach(function (val, idx, array) {
          if (val.name == 'dateFormat') {
            formatter = val.value;
          }
        });
        var form = data
        if (data) {
          form = moment(data, formatter.toUpperCase()).toDate();
        }

        return form;
      }

      ngModel.$parsers.push(into);
      ngModel.$formatters.push(out);


    }
  };
}]);
Widget.directive('orientdatetime', ['Database', function (Database) {


  return {
    restrict: 'A',
    require: 'ngModel',
    link: function (scope, element, attr, ngModel) {

      function into(input) {

        var values = Database.getMetadata()['config']['values'];
        var formatter = undefined;
        values.forEach(function (val, idx, array) {
          if (val.name == 'dateTimeFormat') {
            formatter = val.value;
          }
        });
        var form = input;
        var n = input.getTimezoneOffset();
        if (input) {
          var form = moment(input).formatWithJDF(formatter);
          //var form = moment(input).add('m', n).format('YYYY-MM-DD HH:mm:ss');
        }
        return form;
      }

      function out(data) {
        var form = data
        var values = Database.getMetadata()['config']['values'];
        var formatter = undefined;
        values.forEach(function (val, idx, array) {
          if (val.name == 'dateTimeFormat') {
            formatter = val.value;
          }
        });
        if (data) {
          form = moment(data).formatWithJDF(formatter);
          //form = moment(data).format('DD/MM/YYYY HH:mm:ss');
        }
        return form;
      }

      ngModel.$parsers.push(into);
      ngModel.$formatters.push(out);


    }
  };
}]);
Widget.directive('ridrender', ["Database", "$http", "$compile", function (Database, $http, $compile) {


  return {
    restrict: 'A',
    replace: true,
    link: function (scope, element, attr, ngModel) {

      var value = scope.result[scope.header];


      if (typeof value == 'string') {
        if (value.indexOf('#') == 0) {
          var dbName = Database.getName();
          var link = '<a href="#/database/' + dbName + '/browse/edit/' + value.replace('#', '') + '">' + value + '</a>';
          element.html(link);
        }
      }
      function isRids(value) {
        return (value instanceof Array && value.length > 0 && typeof value[0] == "string" && value[0].indexOf('#') == 0 )
      }


      if (isRids(value)) {


        var LIMIT = 5;
        var PAGE = LIMIT;
        var dbName = Database.getName();

        scope.$new(true);

        scope.expand = function () {
          PAGE += LIMIT;
          renderLimit(PAGE);
        }
        scope.collapse = function () {
          PAGE -= LIMIT;
          renderLimit(PAGE);
        }

        function renderLimit(limit) {
          var i = 0;
          var html = "<div class='rid-list'>";
          value.some(function (elem) {
            if (typeof elem == 'string' && elem.indexOf('#') == 0) {
              var link = '<span class="label label-warning badge-edge"><a href="#/database/' + dbName + '/browse/edit/' + elem.replace('#', '') + '">' + elem + '</a></span> ';
              html += link;

              if (i == PAGE) {
                scope.moreVal = (value.length - 1) - PAGE;
                var expand = '<span class="label label-primary badge-edge"><a ng-click="expand()" href="javascript:void(0)">..More({{moreVal}})</a></span>';
                html += expand;
                return true;
              }
              i++;
              return false;
            }
            return false;
          });
          if (PAGE != LIMIT) {
            var expand = '<span class="label label-primary badge-edge"><a ng-click="collapse()" href="javascript:void(0)">..Less</a></span>';
            html += expand;
          }
          html += "</div>";
          element.html('');
          element.html($compile(html)(scope));
        }

        renderLimit(LIMIT);
      }
      if (scope.header == '@class') {
        var dbName = Database.getName();

        var color = '#428bca';
        if (scope.graphConfig) {
          if (scope.graphConfig.config && scope.graphConfig.config.classes[value]) {
            color = scope.graphConfig.config.classes[value].fill;
          }
        }
        if (value) {
          var link = '<a href="#/database/' + dbName + '/schema/editclass/' + value + '">' + "<span class='label label-primary' style='background-color: " + color + "'> " + value + "</span>" + '</a>';
          element.html(link);
        }
      }

    }
  };
}]);


var count = 0;

Widget.directive('ridrender2', ["Database", "$http", "$compile", function (Database, $http, $compile) {

  return {
    restrict: 'E',
    replace: true,
    transclude: true,
    scope: {
      value: '=value'
    },
    link: function (scope, element, attr, ngModel) {
      var value = scope.value;


      var LIMIT = 5;
      var PAGE = LIMIT;
      var dbName = Database.getName();

      scope.$new(true);

      scope.expand = function () {
        PAGE += LIMIT;
        renderLimit(PAGE);
      }
      scope.collapse = function () {
        PAGE -= LIMIT;
        renderLimit(PAGE);
      }

      function renderLimit(limit) {
        var i = 0;
        var html = "<div class='rid-list'>";
        value.some(function (elem) {
          if (typeof elem == 'string' && elem.indexOf('#') == 0) {
            var link = '<span class="label label-warning badge-edge"><a href="#/database/' + dbName + '/browse/edit/' + elem.replace('#', '') + '">' + elem + '</a></span> ';
            html += link;

            if (i == PAGE) {
              scope.moreVal = (value.length - 1) - PAGE;
              var expand = '<span class="label label-primary badge-edge"><a ng-click="expand()" href="javascript:void(0)">..More({{moreVal}})</a></span>';
              html += expand;
              return true;
            }
            i++;
            return false;
          }
          return false;
        });
        if (PAGE != LIMIT) {
          var expand = '<span class="label label-primary badge-edge"><a ng-click="collapse()" href="javascript:void(0)">..Less</a></span>';
          html += expand;
        }
        html += "</div>";
        element.html('');
        element.html($compile(html)(scope));
      }

      renderLimit(LIMIT);
    }
  };
}]);
Widget.directive('dtpicker', function () {


  return {
    restrict: 'A',
    require: 'ngModel',
    link: function (scope, element, attr, ngModel) {

      element.datetimepicker({
        format: 'dd/MM/yyyy hh:mm:ss',
        language: 'en'
      });
      element.on('changeDate', function (e) {
        ngModel.$setViewValue(e.date);
      });
    }
  };
});
Widget.directive('collaterender', function () {


  return {
    restrict: 'A',
    link: function (scope, element, attr, ngModel) {

      var value = scope.result['collate'];
      if (value == 'ci') {
        scope.result['collate'] = 'Case Insensitive';
      }
    }
  };
});
Widget.provider("$ojson", function () {

  var $jsonProvider = {

    $get: function ($q, $modal, $rootScope) {

      var $ojson = {};

      $ojson.repeat = function (s, count) {
        return new Array(count + 1).join(s);
      }

      $ojson.formatJson = function (json, indentChars) {
        var i = 0,
          il = 0,
          tab = (typeof indentChars !== "undefined") ? indentChars : "    ",
          newJson = "",
          indentLevel = 0,
          inString = false,
          currentChar = null;

        for (i = 0, il = json.length; i < il; i += 1) {
          currentChar = json.charAt(i);

          switch (currentChar) {
            case '{':
            case '[':
              if (!inString) {
                newJson += currentChar + "\r\n" + this.repeat(tab, indentLevel + 1);
                indentLevel += 1;
              } else {
                newJson += currentChar;
              }
              break;
            case '}':
            case ']':
              if (!inString) {
                indentLevel -= 1;
                newJson += "\r\n" + this.repeat(tab, indentLevel) + currentChar;
              } else {
                newJson += currentChar;
              }
              break;
            case ',':
              if (!inString) {
                newJson += ",\r\n" + this.repeat(tab, indentLevel);
              } else {
                newJson += currentChar;
              }
              break;
            case ':':
              if (!inString) {
                newJson += ": ";
              } else {
                newJson += currentChar;
              }
              break;
            case ' ':
            case "\n":
            case "\t":
              if (inString) {
                newJson += currentChar;
              }
              break;
            case '"':
              if (i > 0 && json.charAt(i - 1) !== '\\') {
                inString = !inString;
              }
              newJson += currentChar;
              break;
            default:
              newJson += currentChar;
              break;
          }
        }
        return newJson;
      }
      $ojson.format = function (text) {
        return this.formatJson(text);
      }


      return $ojson;
    }
  }


  return $jsonProvider;
});
Widget.directive('autofill', ["$timeout", function ($timeout) {
  return {
    require: 'ngModel',
    link: function (scope, elem, attrs, ngModel) {
      var origVal = elem.val();
      $timeout(function () {
        var newVal = elem.val();
        if (ngModel.$pristine && origVal !== newVal) {
          ngModel.$setViewValue(newVal);
        }
      }, 500);

      scope.$on("autofill:update", function () {
        ngModel.$setViewValue(elem.val());
      });
    }
  }
}]);
Widget.directive('select', ['$timeout',
  function ($timeout) {
    return {
      restrict: 'E',
      link: function (scope, element, attrs) {
        element.bind('blur', function () {
          element.trigger('change');
        });
      }
    };
  }]);
Widget.directive('whenScrolled', function () {
  return function (scope, elm, attr) {
    var raw = elm[0];

    elm.bind('scroll', function () {
      if (raw.scrollTop + raw.offsetHeight >= raw.scrollHeight) {
        scope.$apply(attr.whenScrolled);
      }
    });
  };
});

Widget.directive('fontpicker', ['$timeout', function ($timeout) {
  return {
    require: 'ngModel',
    link: function (scope, elem, attrs, ngModel) {
      $timeout(function () {
        $(elem).fontIconPicker({
          theme: 'fip-inverted'
        });
      }, 1000)

    }

  }
}]);
// Common directive for "focus"
Widget.directive('focus', ['$timeout',
  function ($timeout) {
    return {
      scope: {
        trigger: '@focus'
      },
      link: function (scope, element) {
        scope.$watch('trigger', function (value) {
          if (value === "true") {
            $timeout(function () {
              element[0].focus();
            }, 100);
          }
        });
      }
    };
  }]);
