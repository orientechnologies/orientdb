import {downgradeInjectable} from '@angular/upgrade/static';
import {CommandService} from './command.service';
import {FormatArrayPipe} from '../pipes';
import {Injectable} from "@angular/core";


declare var angular: any;
declare var d3: any;

@Injectable()
class SchemaService {

  private systems;

  constructor(private commandService: CommandService, private  arrayPipe: FormatArrayPipe) {
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

  createClass(db, {name, abstract, superClasses, clusters}, strict) {

    strict = strict != undefined ? strict : true;

    if (strict) {
      name = `\`${name}\``;
    }
    let query = `CREATE CLASS ${name} `;

    let abstractSQL = abstract ? ' ABSTRACT ' : '';
    let superClassesSQL = '';
    if ((superClasses != null && superClasses.length > 0)) {
      if (strict) {
        superClassesSQL = ' extends ' + this.arrayPipe.transform((superClasses.map((c) => {
            return "`" + c + "`"
          })));
      } else {
        superClassesSQL = ' extends ' + this.arrayPipe.transform(superClasses);
      }
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

  alterClass(db, {clazz, name, value}, strict) {
    strict = strict != undefined ? strict : true;

    if (strict) {
      clazz = `\`${clazz}\``;
    }
    let query = `ALTER CLASS ${clazz} ${name} ${value} `;

    return this.commandService.command({
      db,
      query
    })
  }


  dropClass(db, clazz, strict) {

    strict = strict != undefined ? strict : true;
    if (strict) {
      clazz = `\`${clazz}\``;
    }
    let query = `DROP CLASS ${clazz}  `;
    return this.commandService.command({
      db,
      query
    })
  }


  createProperty(db, {clazz, name, type, linkedType, linkedClass}, strict) {
    strict = strict != undefined ? strict : true;
    linkedType = linkedType || '';
    linkedClass = linkedClass != undefined ? ( strict ? `\`${linkedClass}\`` : linkedClass) : '';

    if (strict) {
      clazz = `\`${clazz}\``;
      name = `\`${name}\``;
    }
    let query = `CREATE PROPERTY ${clazz}.${name} ${type} ${linkedType} ${linkedClass}`;
    return this.commandService.command({
      db,
      query
    })
  }

  alterProperty(db, {clazz, name, entry, value}, strict) {
    strict = strict != undefined ? strict : true;

    if (strict) {
      clazz = `\`${clazz}\``;
      name = `\`${name}\``;
    }
    let query = `ALTER PROPERTY ${clazz}.${name} ${entry} ${value}`;
    return this.commandService.command({
      db,
      query
    })
  }

  dropProperty(db, {clazz, name}, strict) {
    strict = strict != undefined ? strict : true;

    if (strict) {
      clazz = `\`${clazz}\``;
      name = `\`${name}\``;
    }
    let query = `DROP PROPERTY ${clazz}.${name}`;
    return this.commandService.command({
      db,
      query
    })
  }

  createIndex(db, {name, clazz, props, type, engine, metadata}, strict) {

    strict = strict != undefined ? strict : true;

    if (strict) {
      name = `\`${name}\``;
      clazz = `\`${clazz}\``;
    }

    let propsString = this.arrayPipe.transform(props.map((p) => {
      return strict ? `\`${p}\`` : p;
    }));

    let query = `CREATE INDEX  ${name}  ON  ${clazz}  ( ${propsString} )  ${type}`;

    if (engine == 'LUCENE') {
      query += ' ENGINE LUCENE';

      if (metadata) {
        query += ' METADATA ' + metadata;
      }
    }

    return this.commandService.command({
      db,
      query
    })
  }

  dropIndex(db, {name}, strict) {

    strict = strict != undefined ? strict : true;

    if (strict) {
      name = `\`${name}\``;
    }
    let query = `DROP INDEX  ${name}`;
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
    return this.hasSuperClass(classes, clazz, "V") || this.hasSuperClass(classes, clazz, "E");
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

  getSuperClasses(classes, name) {
    return classes.filter((c) => {
      return name == c.name;
    }).map((c) => {
      return c.superClasses;
    }).reduce((a, b) => a.concat(b), []);
  }


  hasSuperClass(classes, source, target) {
    if (source === target) return true;
    let superclasses = this.getSuperClasses(classes, source);
    let found = superclasses.filter((c) => {
        return c === target;
      }).length === 1
    if (!found) {
      found = superclasses.map((c) => {
        return this.hasSuperClass(classes, c, target)
      }).reduce((a, b) => {
        return a || b;
      }, false)
    }
    return found
  }

  isVertexClass(classes, clazz) {
    return this.hasSuperClass(classes, clazz, "V");
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
    return this.hasSuperClass(classes, clazz, "E");
  }


  colors(classes) {
    let val = classes.map((c) => hashCode(c.name));
    val.sort((a, b) => {
      return a - b;
    });
    let color = d3.scale.category20()
      .domain([val[0], val[val.length - 1]])
    return color;
  }


  hash(cls) {
    return hashCode(cls);
  }
}


function hashCode(str) {
  var hash = 0;
  if (str.length == 0) return hash;
  for (let i = 0; i < str.length; i++) {
    let char = str.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash; // Convert to 32bit integer
  }
  return hash;
}

angular.module('schema.services', []).factory(`SchemaService`, downgradeInjectable(SchemaService));

export {SchemaService};
