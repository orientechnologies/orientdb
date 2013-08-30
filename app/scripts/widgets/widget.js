var Widget = angular.module('rendering', []);


Widget.directive('docwidget', function ($compile, $http, Database,CommandApi,DocumentApi) {


    var compileForm = function (response, scope, element, attrs) {
        var formScope = scope.$new(true);
        formScope.doc = scope.doc;
        formScope.database = scope.database;
        formScope.headers = scope.headers;
        formScope.deleteField = scope.deleteField;
        formScope.options = new Array;
        formScope.editorOptions = {
            lineWrapping : true,
            lineNumbers: true,
            readOnly: false,
            mode: 'application/json',
            onChange : function(_editor) {
                console.log(_editor);
            }
        };
        formScope.onLoadEditor = function(_editor){
            console.log(_editor);
        }
        formScope.getTemplate = function (header) {
            if (formScope.doc['@class']) {
                var type = findType(formScope,header)
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
        formScope.requiredString = function (header){
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
        formScope.findAll = function(header){
            var property = Database.listPropertyForClass(formScope.doc['@class'], header);
            if(!formScope.options[header]){
                CommandApi.getAll(scope.database,property.linkedClass,function(data){
                    formScope.options[header] = data.result;
                });
            }
        }
        formScope.$watch('formID.$valid', function (validity) {
            scope.docValid = validity;
        });
        formScope.handleFile = function(header,files){

            var reader = new FileReader();
            reader.onload = function(event) {
                object = {};
                object.filename = files[0].name;
                object.data = event.target.result;
                var blobInput = [event.target.result];
                var blob = new Blob(blobInput);
                formScope.doc[header] =  "$file";
                DocumentApi.uploadFileDocument(formScope.database,formScope.doc,blob,files[0].name);
            };
            reader.readAsDataURL(files[0]);
        }
        var el = angular.element($compile(response.data)(formScope));
        element.empty();
        element.append(el);
    }
    var findType = function(scope,name){
        var type = null;
        var property = Database.listPropertyForClass(scope.doc['@class'],name);

        var guessType = function(value){
            var value = scope.doc[name];
            var type = null;
            if(typeof value === 'number'){
                type = "INTEGER";
            }else if(typeof value === 'boolean') {
                type = "BOOLEAN";
            }
            return type;
        }
        if(!property){
            var fieldTypes = scope.doc['@fieldTypes'];
            if(fieldTypes){
                var found = false;
                fieldTypes.split(",").forEach(function(element,index,array){
                    element.split("=").forEach(function(elem,i,a){
                        if(i==0 && elem == name){
                            found = true;
                            type = Database.getMappingForKey(a[1]);
                        }
                    });
                });
                if(!found){
                    type = guessType(scope.doc[name])
                }
            } else {
                type = guessType(scope.doc[name])
            }
            property = new Object;
            property.name = name;
        }else {
            type = property.type;
        }
        return type !=null ? type : "STRING";
    }
    var linker = function(scope, element, attrs) {
        $http.get( 'views/widget/form.html' ).then(function(response){
            compileForm(response,scope,element,attrs);
        });
    }
    return {
        // A = attribute, E = Element, C = Class and M = HTML Comment
        restrict:'A',
        //The link function is responsible for registering DOM listeners as well as updating the DOM.
        link: linker
    }
});

Widget.directive('jsontext', function() {

    var formatter = (function () {

        function repeat(s, count) {
            return new Array(count + 1).join(s);
        }

        function formatJson(json, indentChars) {
            var i           = 0,
                il          = 0,
                tab         = (typeof indentChars !== "undefined") ? indentChars : "    ",
                newJson     = "",
                indentLevel = 0,
                inString    = false,
                currentChar = null;

            for (i = 0, il = json.length; i < il; i += 1) {
                currentChar = json.charAt(i);

                switch (currentChar) {
                    case '{':
                    case '[':
                        if (!inString) {
                            newJson += currentChar + "\n" + repeat(tab, indentLevel + 1);
                            indentLevel += 1;
                        } else {
                            newJson += currentChar;
                        }
                        break;
                    case '}':
                    case ']':
                        if (!inString) {
                            indentLevel -= 1;
                            newJson += "\n" + repeat(tab, indentLevel) + currentChar;
                        } else {
                            newJson += currentChar;
                        }
                        break;
                    case ',':
                        if (!inString) {
                            newJson += ",\n" + repeat(tab, indentLevel);
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

        return { "formatJson": formatJson };

    }());

    return {
        restrict: 'A',
        require: 'ngModel',
        link: function(scope, element, attr, ngModel) {
            function into(input) {
                return JSON.parse(input);
            }
            function out(data) {
                var string =  JSON.stringify(data);
                return formatter.formatJson(string);
            }
            ngModel.$parsers.push(into);
            ngModel.$formatters.push(out);

        }
    };
});
