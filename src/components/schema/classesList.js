import angular from 'angular';
import Utilities from '../../util/library';


import  template  from './classesList.html';

import '../../views/database/changeNameModal.html';

let SchemaClassesController = function ($scope, $element, $attrs, $location, $modal, $q, $routeParams, Database, ClassAlterApi, CommandApi, Notification) {


  var ctrl = this;

  ctrl.database = $routeParams.database;

  ctrl.$onInit = function () {

  };


  ctrl.queryFilter = null;
  ctrl.$onChanges = (changes) => {
    ctrl.queryFilter = changes.query.currentValue;
    var elem = $element.find('input.form-control')[0];
    elem.val = ctrl.queryFilter;
    angular.element(elem).triggerHandler('input');
  }

  ctrl.clusterStrategies = ['round-robin', "default", "balanced", "local"];

  ctrl.links = {
    linkClasses: Database.getOWikiFor("Schema.html#class"),
    linkClusterSelection: Database.getOWikiFor("Cluster-Selection.html"),
    linkClusters: Database.getOWikiFor("Tutorial-Clusters.html"),
    linkInheritance: Database.getOWikiFor("Inheritance.html")
  }
  ctrl.openClass = function (clazz) {
    $location.path("/database/" + ctrl.database + "/schema/editclass/" + clazz.name);
  }
  ctrl.canDrop = function (clazz) {
    return clazz != "V" && clazz != "E";
  }


  ctrl.queryAll = function (className) {
    $location.path("/database/" + ctrl.database + "/browse/select * from `" + className + "`");
  }


  ctrl.rename = function (cls, event) {

    //modal
    var modalScope = $scope.$new(true);
    modalScope.what = 'class';
    modalScope.tmpName = cls.name;
    var modalPromise = $modal({templateUrl: 'views/database/changeNameModal.html', scope: modalScope, show: false});

    modalScope.rename = function (name) {
      if (name != cls.name) {
        ClassAlterApi.changeProperty($routeParams.database, {
          clazz: cls.name,
          name: "name",
          value: name
        }).then(function (data) {
          var noti = S("The class {{name}} has been renamed to {{newName}}").template({
            name: cls.name,
            newName: name
          }).s;
          Notification.push({content: noti});
          cls.name = name;
        }, function err(data) {
          Notification.push({content: data, error: true});
        });
      }
    }
    modalPromise.$promise.then(modalPromise.show);

  }

  ctrl.dropClass = function (nameClass) {

    Utilities.confirm($scope, $modal, $q, {

      title: 'Warning!',
      body: 'You are dropping class ' + nameClass['name'] + '. Are you sure?',
      success: function () {
        var sql = 'DROP CLASS `' + nameClass['name'] + "`";

        CommandApi.queryText({
          database: $routeParams.database,
          language: 'sql',
          text: sql,
          limit: $scope.limit,
          verbose: false
        }, function (data) {
          var elem = ctrl.classes.indexOf(nameClass);
          ctrl.classes.splice(elem, 1)
          ctrl.classes.splice();
          Notification.push({content: "Class '" + nameClass['name'] + "' dropped."});
        });
      }
    });

  }

  ctrl.createRecord = function (className) {
    $location.path("/database/" + ctrl.database + "/browse/create/" + className);
  }

  ctrl.setClusterStrategy = function (clazz) {
    ClassAlterApi.changeProperty(ctrl.database, {
      clazz: clazz.name,
      name: "clusterSelection",
      value: clazz.clusterSelection
    }).then(function (data) {
      var noti = S("Cluster selection strategy for the class {{name}} has been changed to {{clusterSelection}}").template(clazz).s;
      Notification.push({content: noti});
    }).catch(function (e) {
      Notification.push({content: e, error: true});
    });
  };
}

let INIT = (module) => {

  module.component("classesList", {
    template: template,
    controller: SchemaClassesController,
    bindings: {
      label: "@",
      classes: '<',
      config: '=',
      query: '<',
      onCreate: '&'
    }
  })
}

export default INIT;

