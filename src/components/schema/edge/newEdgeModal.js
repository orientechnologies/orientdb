import  template  from './newEdgeModal.html';


let NewEdgeModalController = function ($scope, $element, $attrs, $location, $modal, $q, $filter, $route, $routeParams, $translate, Database, CommandApi, Notification, SchemaService, FormatErrorPipe) {

  var ctrl = this;


  let handleResponse = (payload) => {
    ctrl.hide();
    $route.reload();
    Notification.push(payload);
  }

  ctrl.hide = $scope.$parent.$hide;


  ctrl.property = {"name": "", "alias": null, "superClasses": ["E"], "abstract": false, "in": null, "out": null}
  ctrl.database = Database;
  ctrl.db = $routeParams.database;
  ctrl.listClasses = SchemaService.edgeClasses(ctrl.database.listClasses()).map((c) => {
    return c.name;
  });

  ctrl.vertices = SchemaService.vertexClasses(ctrl.database.listClasses()).map((c) => {
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

        let promises = [];
        let success = 0;
        let errors = 0;
        let errorString = '';

        let additional = false;
        if (ctrl.property.alias) {
          additional = true;
          let clazz = ctrl.property.name;
          let name = "SHORTNAME";
          let value = `\`${ctrl.property.alias}\``;
          let promise = SchemaService.alterClass(ctrl.db, {
            clazz,
            name,
            value
          });
          promises.push(promise);
        }
        if (ctrl.property.in) {
          additional = true;
          let clazz = ctrl.property.name;
          let name = "in";
          let type = "LINK";
          let linkedClass = ctrl.property.in;
          promises.push(SchemaService.createProperty(ctrl.db, {clazz, name, type, linkedClass}));
        }
        if (ctrl.property.out) {
          additional = true;
          let clazz = ctrl.property.name;
          let name = "out";
          let type = "LINK";
          let linkedClass = ctrl.property.out;
          promises.push(SchemaService.createProperty(ctrl.db, {clazz, name, type, linkedClass}));
        }
        promises.forEach((promise) => {
          promise.then(() => {
            success++;
            if (success + errors == promises.length) {
              if (errorString === '') {
                handleResponse({content: "Edge Class '" + ctrl.property['name'] + "' correctly created."});
              } else {
                handleResponse({
                  content: "Edge Class '" + ctrl.property['name'] + "'created with warning : " + errorString,
                  warning: true,
                  sticky: true
                })
              }
            }
          }).catch((err) => {
            errors++;
            errorString += "<br>" + FormatErrorPipe.transform(err.json());
            if (success + errors == promises.length) {
              handleResponse({
                content: "Edge Class '" + ctrl.property['name'] + "'created with warning : " + errorString,
                warning: true,
                sticky: true
              })
            }
          })
        })

        if (!additional) {
          handleResponse({content: "Edge Class '" + ctrl.property['name'] + "' correctly created."})
        }
      }).catch((err) => {
      ctrl.testMsgClass = 'alert alert-danger'
      ctrl.testMsg = FormatErrorPipe.transform(err.json());
    });
  }
}


let INIT = (module) => {

  module.component("newEdgeModal", {
    template: template,
    controller: NewEdgeModalController,
    bindings: {}
  })

}

export default INIT;
