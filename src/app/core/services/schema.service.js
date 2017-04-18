"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var static_1 = require("@angular/upgrade/static");
var core_1 = require("@angular/core");
var SchemaService = (function () {
    function SchemaService(commandService, arrayPipe) {
        this.commandService = commandService;
        this.arrayPipe = arrayPipe;
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
    SchemaService.prototype.createClass = function (db, _a, strict) {
        var name = _a.name, abstract = _a.abstract, superClasses = _a.superClasses, clusters = _a.clusters;
        strict = strict != undefined ? strict : true;
        if (strict) {
            name = "`" + name + "`";
        }
        var query = "CREATE CLASS " + name + " ";
        var abstractSQL = abstract ? ' ABSTRACT ' : '';
        var superClassesSQL = '';
        if ((superClasses != null && superClasses.length > 0)) {
            if (strict) {
                superClassesSQL = ' extends ' + this.arrayPipe.transform((superClasses.map(function (c) {
                    return "`" + c + "`";
                })));
            }
            else {
                superClassesSQL = ' extends ' + this.arrayPipe.transform(superClasses);
            }
        }
        var clusterSQL = '';
        if (clusters) {
            clusterSQL = "CLUSTERS " + clusters;
        }
        query = query + superClassesSQL + clusterSQL + abstractSQL;
        return this.commandService.command({
            db: db,
            query: query
        });
    };
    SchemaService.prototype.alterClass = function (db, _a, strict) {
        var clazz = _a.clazz, name = _a.name, value = _a.value;
        strict = strict != undefined ? strict : true;
        if (strict) {
            clazz = "`" + clazz + "`";
        }
        var query = "ALTER CLASS " + clazz + " " + name + " " + value + " ";
        return this.commandService.command({
            db: db,
            query: query
        });
    };
    SchemaService.prototype.dropClass = function (db, clazz, strict) {
        strict = strict != undefined ? strict : true;
        if (strict) {
            clazz = "`" + clazz + "`";
        }
        var query = "DROP CLASS " + clazz + "  ";
        return this.commandService.command({
            db: db,
            query: query
        });
    };
    SchemaService.prototype.createProperty = function (db, _a, strict) {
        var clazz = _a.clazz, name = _a.name, type = _a.type, linkedType = _a.linkedType, linkedClass = _a.linkedClass;
        strict = strict != undefined ? strict : true;
        linkedType = linkedType || '';
        linkedClass = linkedClass != undefined ? (strict ? "`" + linkedClass + "`" : linkedClass) : '';
        if (strict) {
            clazz = "`" + clazz + "`";
            name = "`" + name + "`";
        }
        var query = "CREATE PROPERTY " + clazz + "." + name + " " + type + " " + linkedType + " " + linkedClass;
        return this.commandService.command({
            db: db,
            query: query
        });
    };
    SchemaService.prototype.alterProperty = function (db, _a, strict) {
        var clazz = _a.clazz, name = _a.name, entry = _a.entry, value = _a.value;
        strict = strict != undefined ? strict : true;
        if (strict) {
            clazz = "`" + clazz + "`";
            name = "`" + name + "`";
        }
        var query = "ALTER PROPERTY " + clazz + "." + name + " " + entry + " \"" + value + "\"";
        return this.commandService.command({
            db: db,
            query: query
        });
    };
    SchemaService.prototype.dropProperty = function (db, _a, strict) {
        var clazz = _a.clazz, name = _a.name;
        strict = strict != undefined ? strict : true;
        if (strict) {
            clazz = "`" + clazz + "`";
            name = "`" + name + "`";
        }
        var query = "DROP PROPERTY " + clazz + "." + name;
        return this.commandService.command({
            db: db,
            query: query
        });
    };
    SchemaService.prototype.createIndex = function (db, _a, strict) {
        var name = _a.name, clazz = _a.clazz, props = _a.props, type = _a.type, engine = _a.engine, metadata = _a.metadata;
        strict = strict != undefined ? strict : true;
        if (strict) {
            name = "`" + name + "`";
            clazz = "`" + clazz + "`";
        }
        var propsString = this.arrayPipe.transform(props.map(function (p) {
            return strict ? "`" + p + "`" : p;
        }));
        var query = "CREATE INDEX  " + name + "  ON  " + clazz + "  ( " + propsString + " )  " + type;
        if (engine == 'LUCENE') {
            query += ' ENGINE LUCENE';
            if (metadata) {
                query += ' METADATA ' + metadata;
            }
        }
        return this.commandService.command({
            db: db,
            query: query
        });
    };
    SchemaService.prototype.dropIndex = function (db, _a, strict) {
        var name = _a.name;
        strict = strict != undefined ? strict : true;
        if (strict) {
            name = "`" + name + "`";
        }
        var query = "DROP INDEX  " + name;
        return this.commandService.command({
            db: db,
            query: query
        });
    };
    SchemaService.prototype.isSystemClass = function (c) {
        return this.systems.indexOf(c) != -1;
    };
    SchemaService.prototype.genericClasses = function (classes) {
        var _this = this;
        return classes.filter(function (c) {
            return !_this.isGraphClass(classes, c.name);
        });
    };
    SchemaService.prototype.isGraphClass = function (classes, clazz) {
        return this.hasSuperClass(classes, clazz, "V") || this.hasSuperClass(classes, clazz, "E");
    };
    SchemaService.prototype.getSuperClazz = function (classes, clazz) {
        var clazzReturn = "";
        for (var entry in classes) {
            var name = classes[entry]['name'];
            if (clazz == name) {
                clazzReturn = classes[entry].superClass;
                break;
            }
        }
        return clazzReturn;
    };
    SchemaService.prototype.getSuperClasses = function (classes, name) {
        return classes.filter(function (c) {
            return name == c.name;
        }).map(function (c) {
            return c.superClasses;
        }).reduce(function (a, b) { return a.concat(b); }, []);
    };
    SchemaService.prototype.hasSuperClass = function (classes, source, target) {
        var _this = this;
        if (source === target)
            return true;
        var superclasses = this.getSuperClasses(classes, source);
        var found = superclasses.filter(function (c) {
            return c === target;
        }).length === 1;
        if (!found) {
            found = superclasses.map(function (c) {
                return _this.hasSuperClass(classes, c, target);
            }).reduce(function (a, b) {
                return a || b;
            }, false);
        }
        return found;
    };
    SchemaService.prototype.isVertexClass = function (classes, clazz) {
        return this.hasSuperClass(classes, clazz, "V");
    };
    SchemaService.prototype.vertexClasses = function (classes) {
        var _this = this;
        return classes.filter(function (c) {
            return _this.isVertexClass(classes, c.name);
        });
    };
    SchemaService.prototype.edgeClasses = function (classes) {
        var _this = this;
        return classes.filter(function (c) {
            return _this.isEdgeClass(classes, c.name);
        });
    };
    SchemaService.prototype.isEdgeClass = function (classes, clazz) {
        return this.hasSuperClass(classes, clazz, "E");
    };
    SchemaService.prototype.colors = function (classes) {
        var val = classes.map(function (c) { return hashCode(c.name); });
        val.sort(function (a, b) {
            return a - b;
        });
        var color = d3.scale.category20()
            .domain([val[0], val[val.length - 1]]);
        return color;
    };
    SchemaService.prototype.hash = function (cls) {
        return hashCode(cls);
    };
    return SchemaService;
}());
SchemaService = __decorate([
    core_1.Injectable()
], SchemaService);
exports.SchemaService = SchemaService;
function hashCode(str) {
    var hash = 0;
    if (str.length == 0)
        return hash;
    for (var i = 0; i < str.length; i++) {
        var char = str.charCodeAt(i);
        hash = ((hash << 5) - hash) + char;
        hash = hash & hash; // Convert to 32bit integer
    }
    return hash;
}
angular.module('schema.services', []).factory("SchemaService", static_1.downgradeInjectable(SchemaService));
