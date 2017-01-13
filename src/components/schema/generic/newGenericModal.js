import  template  from './newGenericModal.html';


let NewGenericModalController = function ($scope, $element, $attrs, $location, $modal, $q, $filter, $route, $routeParams, $translate, Database, CommandApi, Notification, SchemaService, FormatErrorPipe) {

  var ctrl = this;


  let handleResponse = (payload) => {
    ctrl.hide();
    $route.reload();
    Notification.push(payload);
  }

  ctrl.hide = $scope.$parent.$hide;


  ctrl.property = {"name": "", "alias": null, "superClasses": [], "abstract": false}
  ctrl.database = Database;
  ctrl.db = $routeParams.database;
  ctrl.listClasses = SchemaService.genericClasses(ctrl.database.listClasses()).map((c) => {
    return c.name;
  });

  ctrl.links = {
    linkClusters: Database.getOWikiFor("Tutorial-Clusters.html")
  }
  $translate("class.clusters", ctrl.links).then(function (data) {
    ctrl.hint = data;
  });

  ctrl.saveNewClass = function () {
    SchemaService.createClass(ctrl.db, ctrl.property)
      .then((res) => {
        if (ctrl.property.alias) {
          let clazz = ctrl.property.name;
          let name = "SHORTNAME";
          let value = `\`${ctrl.property.alias}\``;
          SchemaService.alterClass(ctrl.db, {
            clazz,
            name,
            value
          }).then(() => {
            handleResponse({content: "Class '" + ctrl.property['name'] + "' correctly created."})
          }).catch((err) => {
            handleResponse({
              content: "Class '" + ctrl.property['name'] + "'created with warning : " + FormatErrorPipe.transform(err.json()),
              warning: true
            })
          })
        } else {
          handleResponse({content: "Class '" + ctrl.property['name'] + "' correctly created."})
        }
      }).catch((err) => {
      ctrl.testMsgClass = 'alert alert-danger'
      ctrl.testMsg = FormatErrorPipe.transform(err.json());
    });
  }
}


let INIT = (module) => {

  module.component("newGenericModal", {
    template: template,
    controller: NewGenericModalController,
    bindings: {}
  })

}

export default INIT;
