import {downgradeInjectable} from '@angular/upgrade/static';
import {CommandService} from './command.service';
import {FormatArrayPipe} from '../pipes';

class SchemaService {
  constructor(commandService, arrayPipe) {
    this.commandService = commandService;
    this.arrayPipe = arrayPipe;

    this.systems = ["OUser",
      "ORole",
      "OIdentity",
      "_studio",
      "OFunction",
      "ORestricted",
      "OShape",
      "OPoint",
      "OGeometryCollection",
      "OLineString",
      "OMultiLineString",
      "OMultiPoint",
      "OPolygon",
      "OMultiPolygon",
      "ORectangle",
      "OSchedule",
      "OSequence",
      "OTriggered"];
  }

  createClass(db, {name, abstract, superClasses, clusters}) {

    let query = 'CREATE CLASS `' + name + "`";
    let abstractSQL = abstract ? ' ABSTRACT ' : '';
    let superClassesSQL = '';
    if ((superClasses != null && superClasses.length > 0)) {
      superClassesSQL = ' extends ' + this.arrayPipe.transform((superClasses.map((c) => {
          return "`" + c + "`"
        })));
    }
    let clusterSQL = '';
    if (clusters) {
      clusterSQL = `CLUSTERS ${clusters}`;
    }

    query = query + superClassesSQL + clusterSQL + abstractSQL;
    return this.commandService.command({
      db,
      query
    })
  }

  alterClass(db, {clazz, name, value}) {
    let query = `ALTER CLASS \`${clazz}\` ${name} ${value} `;
    return this.commandService.command({
      db,
      query
    })
  }


  createProperty(db, {clazz, name, type, linkedType, linkedClass}) {

    linkedType = linkedType || '';
    linkedClass = linkedClass || '';
    let query = `CREATE PROPERTY \`${clazz}\`.\`${name}\` ${type} ${linkedType} ${linkedClass}`;
    return this.commandService.command({
      db,
      query
    })
  }

  isSystemClass(c) {
    return this.systems.indexOf(c) != -1;
  }

  genericClasses(classes) {
    return classes.filter((c) => {
      return !this.isGraphClass(classes, c.name);
    })
  }

  isGraphClass(classes, clazz) {
    let sup = clazz;
    let iterator = clazz;
    while ((iterator = this.getSuperClazz(classes, iterator)) != "") {
      sup = iterator;
      if (sup == 'V' || sup == 'E') {
        return true;
      }
    }
    return sup == 'V' || sup == 'E';
  }

  getSuperClazz(classes, clazz) {
    let clazzReturn = "";
    for (var entry in classes) {
      var name = classes[entry]['name'];
      if (clazz == name) {
        clazzReturn = classes[entry].superClass;
        break;
      }
    }
    return clazzReturn;
  }


  isVertexClass(classes, clazz) {
    var sup = clazz;
    var iterator = clazz;
    while ((iterator = this.getSuperClazz(classes, iterator)) != "") {
      sup = iterator;
    }
    return sup == 'V';
  }

  vertexClasses(classes) {
    return classes.filter((c) => {
      return this.isVertexClass(classes, c.name);
    })
  }

  edgeClasses(classes) {
    return classes.filter((c) => {
      return this.isEdgeClass(classes, c.name);
    })
  }

  isEdgeClass(classes, clazz) {
    var sup = clazz;
    var iterator = clazz;
    while ((iterator = this.getSuperClazz(classes, iterator)) != "") {
      sup = iterator;
    }
    return sup == 'E';
  }
}

SchemaService
  .parameters = [CommandService, FormatArrayPipe];


angular.module('schema.services', []).factory(`SchemaService`, downgradeInjectable(SchemaService));

export {SchemaService};
