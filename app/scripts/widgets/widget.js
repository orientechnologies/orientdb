var Widget = angular.module('rendering', []);

Widget.directive('render', function($compile,Database) {
  



  var getTemplate = function(scope,name,element,ngModel) {
    var tpl;
    var type = Database.findType(scope.doc['@class'],scope.doc[name],name);
   
    switch(type){          
          case "STRING":
           tpl = getStringTemplate(element);
          break;
          case "INTEGER":
          tpl = getNumberTemplate(element);
          break;
           case "DATE":
           var format = Database.getDateFormat();
          tpl = getDateTemplate(element,format);           
           break;
          case "DATETIME":
          var format = Database.getDateTimeFormat();
           tpl = getDateTimeTemplate(element,format);
            
          break;
        }
        return tpl;
  }

  var linker = function(scope, element, attrs,ngModel) {

      attrs.$observe('render-field',function(){
        var name = element.attr('render-field') ? element.attr('render-field') : "text";
        var el = angular.element($compile(getTemplate(scope,name,element,ngModel))(scope));
        element.append(el);
      });
      
  }

  var getStringTemplate = function(element){
    return "<textarea class='input-xlarge' ng-model='" + element.attr('ng-model') + "' ></textarea>";
  };
  var getNumberTemplate = function(element){
    return "<input type='text' class='input-xlarge' ng-model='" + element.attr('ng-model') + "' />";
  };
  var getDateTemplate = function (element,format){
    return "<input type='text' class='input-xlarge' ng-model='" + element.attr('ng-model') + "' data-date-type='string' data-date-format='" + format+ "' bs-datepicker>";
  };
  var getDateTimeTemplate = function(element,format){
    return "<input type='text' class='input-xlarge' ng-model='" + element.attr('ng-model') + "' data-date-type='string' data-date-format='"+format+"' bs-datepicker>";
  };
  return {
    // A = attribute, E = Element, C = Class and M = HTML Comment
    require: 'ngModel',
    restrict:'A',
    //The link function is responsible for registering DOM listeners as well as updating the DOM.
    link: linker
  }
});