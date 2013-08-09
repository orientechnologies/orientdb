var Widget = angular.module('rendering', []);

Widget.directive('docform', function($compile,Database) {

  var getTemplate = function(scope,name,element) {
    var tpl;
    var type = "STRING";
    var property = Database.listPropertyForClass(scope.doc['@class'],name);
    if(!property){
      var fieldTypes = scope.doc['@fieldTypes'];
      if(fieldTypes){
        var found = true;
        fieldTypes.split(",").forEach(function(element,index,array){
          element.split("=").forEach(function(elem,i,a){
           if(i==0 && elem == name){
            type = Database.getMappingForKey(a[1]); 
          }
        });
        });
        if(!found){
          var value = scope.doc[name];
          if(typeof value === 'number'){
           type = "INTEGER";
         }else if(typeof value === 'boolean') {
           type = "BOOLEAN";
         }
       }
     } else {
      var value = scope.doc[name];
      if(typeof value === 'number'){
       type = "INTEGER";
     }else if(typeof value === 'boolean') {
       type = "BOOLEAN";
     }
   }
   property = new Object;
   property.name = name;
 }else {
  type = property.type;
}
switch(type){          
  
  case "INTEGER":
  tpl = getNumberTemplate(property);
  break;
  case "DATE":
  var format = Database.getDateFormat();
  tpl = getDateTemplate(format,property);           
  break;
  case "DATETIME":
  var format = Database.getDateTimeFormat();
  tpl = getDateTimeTemplate(format,property);
  break;
  case "BOOLEAN":
  tpl = getBooleanTemplate(property);
  break;
  case "LINK":
  tpl = getLinkTemplate(property);
  break;
  default: 
    tpl = getStringTemplate(property);
    break;
}
var select = "<select class='span3 form-control' ng-disabled='true'><option value='" +type+"''>" + type+ "</option></select>";
var del = "<a href='javascript:void(0)'' class='btn btn-mini pull-right' tooltip='Delete field' ng-click='deleteField(\"" + name + "\")' ><i class='icon-trash'></i></a>";
return  tpl +  select + del ;
}

var linker = function(scope, element, attrs) {


  scope.$watch('formID.$valid', function(validity) {
    scope.docValid = validity;
  });
  scope.$watch('headers.length',function(h){
    var docHtml = '<ng-form name="formID">' //{{formID.$valid}}
    angular.forEach(scope.headers,function(el){
      docHtml = docHtml +'<div class="control-group" ><label class="control-label">'+ el +' </label> <div class="controls controls-row" >' + getTemplate(scope,el,element) + '</div></div>'
    });
    docHtml = docHtml + '</ng-form>';
    var el = angular.element($compile(docHtml)(scope));
    element.empty();
    element.append(el);
  });
}

var getStringTemplate = function(property){
  var required = property.mandatory ? "required" : "";
  var tpl = "<textarea class='input-xlarge span6' " + required + " ng-model='doc[\""+ property.name + "\"]' ></textarea>";
  return tpl;
};
var getBooleanTemplate = function(property){
  var required = property.mandatory ? "required" : "";
  var tpl = "<input type='checkbox' class='input-xlarge span6' name='" +property.name+ "'  "+ required +" ng-model='doc[\""+ property.name +"\"]' />";
  return tpl;
};
var getNumberTemplate = function(property){
  var required = property.mandatory ? "required" : "";
  var tpl = "<input type='number' name='" +property.name+ "' class='input-xlarge span6'  "+ required +" ng-model='doc[\""+ property.name +"\"]' />";
  return tpl;
};
var getDateTemplate = function (format,property){
  return "<input type='text' class='input-xlarge span6' ng-model='doc[\""+ property.name +"\"]' data-date-type='string' data-date-format='" + format+ "' bs-datepicker>";
};
var getDateTimeTemplate = function(format,property){
  return "<input type='text' class='input-xlarge span6' ng-model='doc[\""+ property.name +"\"]' data-date-type='string' data-date-format='"+format+"' bs-datepicker>";
};
var getLinkTemplate = function(property){
  var required = property.mandatory ? "required" : "";

  var tpl = "<select ui-select2 class='input-xlarge span6' ng-model='doc[\""+ property.name +"\"]' data-placeholder='@rid'></select>";
  return tpl;
};
return {
    // A = attribute, E = Element, C = Class and M = HTML Comment
    restrict:'A',
    //The link function is responsible for registering DOM listeners as well as updating the DOM.
    link: linker
  }
});