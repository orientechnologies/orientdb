import 'angular-resource';
import {API, STUDIO_VERSION} from '../constants';
import  Base64  from 'js-base64';
import SpinnerService from './spinner-service';
import angular from 'angular';

let database = angular.module('database.services', ['ngResource', SpinnerService]);


database.factory('Database', ["DatabaseApi", "localStorageService", function (DatabaseApi, localStorageService) {


  var version = STUDIO_VERSION.indexOf("SNAPSHOT") == -1 ? STUDIO_VERSION : "last";
  var wikiBase = 'http://www.orientdb.com/docs/' + version + "/";
  var oWikiBase = 'http://www.orientdb.com/docs/' + version + "/";
  var current = {
    name: null,
    username: null,
    metadata: null,
    wiki: wikiBase,
    oWiki: oWikiBase,
    version: version

  }
  return {

    header: ["@rid", "@version", "@class"],

    exclude: ["@type", "@fieldTypes", "$then", "$resolved", "$promise"],

    listTypes: ['BINARY', 'BYTE', 'BOOLEAN', 'EMBEDDED', 'EMBEDDEDLIST', 'EMBEDDEDMAP', 'EMBEDDEDSET', 'DECIMAL', 'FLOAT', 'DATE', 'DATETIME', 'DOUBLE', 'INTEGER', 'LINK', 'LINKLIST', 'LINKMAP', 'LINKSET', 'LONG', 'SHORT', 'STRING'],

    mapping: {
      'BINARY': 'b',
      'BYTE': 'b',
      'DATE': 'a',
      'DATETIME': 't',
      'FLOAT': 'f',
      'DECIMAL': 'c',
      'LONG': 'l',
      'DOUBLE': 'd',
      'SHORT': 's',
      'LINKSET': 'e',
      'EMBEDDEDLIST': 'e',
      'EMBEDDEDSET': 'e'

    },
    getMetadata: function () {
      /*if (current.metadata == null) {
       var tmp = localStorageService.get("CurrentDB");
       if (tmp != null) current = tmp;
       } */
      return current.metadata;
    },
    setMetadata: function (metadata) {
      current.metadata = metadata;
    },
    getWiki: function () {
      return current.wiki;
    },
    setWiki: function (urlWiki) {
      current.wiki = wikiBase + urlWiki;
    },
    resolveWiki: function (urlWiki) {
      return wikiBase + urlWiki;
    },
    getMappingFor: function (type) {
      return this.mapping[type];
    },
    getMappingForKey: function (key) {
      var self = this;
      var type = undefined;
      Object.keys(this.mapping).forEach(function (elem, index, array) {
        if (self.mapping[elem] == key) {
          type = elem;
        }
      });

      return type;
    },
    currentUser: function () {
      return current != null ? current.username : null;
    },
    setCurrentUser: function (user) {
      current.username = user;
    },
    setName: function (name) {
      current.name = name;
    },
    getName: function () {
      return current != null ? current.name : current;
    },
    getSupportedTypes: function () {
      return this.listTypes;
    },
    refreshMetadata: function (database, callback) {
      var currentDb = DatabaseApi.get({database: database}, function () {


        current.name = database;
        current.username = currentDb.currentUser || "root";
        current.metadata = currentDb;
        localStorageService.add("CurrentDB", current);
        if (callback)
          callback();
      });
    },
    isConnected: function () {
      return current.username != null;
    },
    connect: function (database, username, password, callback, error) {
      var self = this;
      DatabaseApi.connect(database, username, password, function () {
        callback();
      }, function () {
        error();
      });
    },

    disconnect: function (callback) {
      DatabaseApi.disconnect(function () {
        delete current.name;
        delete current.username;
        delete current.metadata;
        //if (localStorageService.cookie)
        //    localStorageService.cookie.clearAll();
        document.cookie = "";

        callback();
      });

    },
    findType: function (clazz, value, field) {
      var metadata = this.getMetadata();
      if (metadata == null) return "STRING";
      var classes = metadata['classes'];
      for (var entry in classes) {
        if (clazz.toUpperCase() == classes[entry].name.toUpperCase()) {
          var props = classes[entry]['properties'];
          for (var f in props) {
            if (field == props[f].name) {
              return props[f].type;
            }
          }
          ;
        }
      }
      var type = "STRING";
      if (typeof value === 'number') {
        type = "INTEGER";
      }
      return type;
    },
    getDateFormat: function () {
      return "yyyy-mm-dd"
    },
    getDateTimeFormat: function () {
      return "yyyy-mm-dd HH:mm:ss";
    },
    getFieldType: function (clazz, field) {
      var metadata = this.getMetadata();
      var classes = metadata['classes'];
      var type = undefined;
      classes.forEach(function (element, index, array) {
        if (element.name.toUpperCase() == clazz.toUpperCase()) {
          if (element['properties']) {
            element['properties'].forEach(function (element, index, array) {
              if (element.name == field) {
                type = element.type;
              }
            });
          }
        }
      });
      return type;
    },
    isLink: function (type) {
      return type == "LINKSET" || type == "LINKLIST";
    },
    listField: function (clazz) {
      var metadata = this.getMetadata();
      var classes = metadata['classes'];
      var fields = new Array
      for (var entry in classes) {
        var defaultCluster = classes[entry]['defaultCluster'];
        if (clazz.toUpperCase() == classes[entry].name.toUpperCase()) {
          var props = classes[entry]['properties'];
          for (var f in props) {
            fields.push(props[f].name);
          }
          ;
          break;
        }
      }
      return fields;
    },
    listPropertiesForClass: function (clazz) {
      var metadata = this.getMetadata();
      var classes = metadata['classes'];
      var fields = new Array
      for (var entry in classes) {
        var defaultCluster = classes[entry]['properties'];
        if (clazz.toUpperCase() == classes[entry].name.toUpperCase()) {
          var props = classes[entry]['properties'];
          for (var f in props) {
            fields.push(props[f]);
          }
          ;
          break;
        }
      }
      return fields;
    },
    listPropertyForClass: function (clazz, field) {
      var metadata = this.getMetadata();
      var classes = metadata['classes'];
      var property = undefined;
      classes.forEach(function (element, index, array) {
        if (element.name.toUpperCase() == clazz.toUpperCase()) {
          if (element['properties']) {
            element['properties'].forEach(function (element, index, array) {
              if (element.name == field) {
                property = element;
              }
            });
          }
        }
      });
      return property;
    },
    listIndexesForClass: function (clazz) {
      var metadata = this.getMetadata();
      var classes = metadata['classes'];
      var fields = new Array
      for (var entry in classes) {
        var defaultCluster = classes[entry]['indexes'];
        if (clazz.toUpperCase() == classes[entry].name.toUpperCase()) {
          var props = classes[entry]['indexes'];
          for (var f in props) {

            fields.push(props[f]);
          }
          ;
          break;
        }
      }
      return fields;
    },
    listClasses: function () {
      var metadata = this.getMetadata();
      var classes = metadata['classes'];
      var fields = new Array
      for (var entry in classes) {
        var claq = classes[entry].name
        fields.push(classes[entry])
      }
      return fields;
    },
    hasClass: function (clazz) {
      var metadata = this.getMetadata();
      var classes = metadata['classes'];
      for (var entry in classes) {
        var claq = classes[entry].name
        if (claq == clazz) {
          return true;
        }
      }
      return false;
    },
    listNameOfClasses: function () {
      var metadata = this.getMetadata();
      var classes = metadata['classes'];
      var fields = new Array
      for (var entry in classes) {
        var claq = classes[entry]['name']
        fields.push(claq)
      }
      return fields;
    },
    listNameOfProperties: function () {
      var metadata = this.getMetadata();
      var classes = metadata['classes'];
      var fields = new Array

      classes.forEach(function (element, index, array) {
        if (element['properties']) {
          element['properties'].forEach(function (element, index, array) {
            fields.push(element.name);
          });
        }
      });

      return fields;
    },
    classFromCluster: function (cluster) {
      var metadata = this.getMetadata();
      var classes = metadata['classes'];
      var clazz;
      for (var entry in classes) {
        var defaultCluster = classes[entry]['defaultCluster'];
        if (cluster == defaultCluster) {
          clazz = classes[entry].name;
          break;
        }
      }
      return clazz;
    },
    getSuperClazz: function (clazz) {
      var metadata = this.getMetadata();
      var classes = metadata['classes'];
      var clazzReturn = "";
      for (var entry in classes) {
        var name = classes[entry]['name'];
        if (clazz == name) {
          clazzReturn = classes[entry].superClass;
          break;
        }
      }
      return clazzReturn;
    },
    isGraph: function (clazz) {
      var sup = clazz;

      var iterator = clazz;
      while ((iterator = this.getSuperClazz(iterator)) != "") {
        sup = iterator;
        if (sup == 'V' || sup == 'E') {
          return true;
        }
      }
      return sup == 'V' || sup == 'E';
    },
    isVertex: function (clazz) {
      var sup = clazz;
      var iterator = clazz;

      while ((iterator = this.getSuperClazz(iterator)) != "") {
        sup = iterator;
      }
      return sup == 'V';
    },
    isEdge: function (clazz) {
      var sup = clazz;
      var iterator = clazz;
      while ((iterator = this.getSuperClazz(iterator)) != "") {
        sup = iterator;
      }
      return sup == 'E';
    },
    getClazzEdge: function () {
      var metadata = this.getMetadata();
      var classes = metadata['classes'];
      var clazzes = new Array;
      for (var entry in classes) {
        var name = classes[entry]['name'];
        if (this.isEdge(name)) {
          clazzes.push(name);
        }
      }
      return clazzes;
    },
    getClazzVertex: function () {
      var metadata = this.getMetadata();
      var classes = metadata['classes'];
      var clazzes = new Array;
      for (var entry in classes) {
        var name = classes[entry]['name'];
        if (this.isVertex(name)) {
          clazzes.push(name);
        }
      }
      return clazzes;
    },
    /**
     * Creates a new Array from a document with property name.
     *
     * @param {doc} OrientDB Document.
     * @return {Array} Property name Array.
     */
    getPropertyFromDoc: function (doc) {
      var c = doc['@class'];
      var isGraph = this.isGraph(c);
      var fixedHeader = this.header.concat(this.exclude);
      var self = this;
      var fields = this.listField(c);
      var all = Object.keys(doc).filter(function (element, index, array) {
        var type = self.getFieldType(c, element);
        if (isGraph) {
          return (fixedHeader.indexOf(element) == -1 && (!element.startsWith("in_") && !element.startsWith("out_")) && !self.isLink(type));
        } else {
          return (fixedHeader.indexOf(element) == -1 && !self.isLink(type));
        }
      });
      var toAdd = new Array;
      fields.forEach(function (elem, index, array) {
        if (all.indexOf(elem) == -1) {
          var type = self.getFieldType(c, elem);
          var bool = true;
          if (isGraph) {
            bool = (fixedHeader.indexOf(elem) == -1 && (!elem.startsWith("in_") && !elem.startsWith("out_")) && !self.isLink(type));
          } else {
            bool = (fixedHeader.indexOf(elem) == -1 && !self.isLink(type));
          }
          if (bool)
            toAdd.push(elem);
        }
      })
      return all.concat(toAdd);
    },
    getEdge: function (doc, direction) {

      var all = Object.keys(doc).filter(function (element, index, array) {
        return element.startsWith(direction);
      });
      return all;
    },
    findTypeFromFieldTipes: function (doc, name) {
      var fieldTypes = doc['@fieldTypes'];
      var type = undefined;
      var self = this;
      if (fieldTypes) {
        fieldTypes.split(",").forEach(function (element, index, array) {
          element.split("=").forEach(function (elem, i, a) {
            if (i == 0 && elem == name) {
              type = self.getMappingForKey(a[1]);
            }
          });
        });
      }
      return type;
    },
    getLink: function (doc, exclude) {
      var self = this;
      var all = Object.keys(doc).filter(function (element, index, array) {
        var type = self.getFieldType(doc['@class'], element);
        if (!type) {
          type = self.findTypeFromFieldTipes(doc, element);
        }
        if (exclude) {
          return self.isLink(type) && exclude.indexOf(element) == -1;
        } else {
          return self.isLink(type);
        }
      });
      return all;
    },
    /**
     * Creates a new Array with property name from a result set of documents.
     *
     * @param {results} OrientDB result set.
     * @return {Array} Property name Array.
     */
    getPropertyTableFromResults: function (results) {
      var self = this;
      var headers = new Array;
      var temp = false;
      results.forEach(function (element, index, array) {
        var tmp = Object.keys(element);
        if (headers.length == 0) {
          if (element["@rid"] && element["@rid"].startsWith('#-')) {
            temp = true;
          }
          headers = headers.concat(tmp);
        } else {
          var tmp2 = tmp.filter(function (element, index, array) {
            return headers.indexOf(element) == -1;
          });
          headers = headers.concat(tmp2);
        }
      });
      var all = headers.filter(function (element, index, array) {
        return self.exclude.indexOf(element) == -1;
      });
      if (temp) {
        if (all.indexOf("@rid") != -1)
          all.splice(all.indexOf("@rid"), 1)
        if (all.indexOf("@version") != -1)
          all.splice(all.indexOf("@version"), 1)

      }
      return all;
    },
    getVersion: function () {
      return current.version;
    },
    getOWikiFor: function (page) {
      return current.oWiki + page;
    },
    isStrictSql: function () {

      var strictSql = false;
      var properties = current.metadata['config']['properties'];
      properties.forEach(function (p) {
        if (p.name == 'strictSql') {
          strictSql = p.value == "true";
        }
      })
      return strictSql;
    }


  };
}]);

database.factory('DatabaseApi', ["$http", "$resource", "$q", function ($http, $resource, $q) {


  var urlWiki = "https://github.com/orientechnologies/orientdb-studio/wiki/Functions";

  var resource = $resource(API + 'database/:database');

  resource.sso = false;


  resource.isSSO = function () {
    var deferred = $q.defer();

    $http.get(API + 'sso').success(function (data) {
      resource.sso = data.enabled;
      deferred.resolve(data);
    }).error(function (data) {
      deferred.reject(data);
    });

    return deferred.promise;
  }
  resource.listDatabases = function (callback) {
    var deferred = $q.defer();
    $http.get(API + 'listDatabases').success(function (data) {
      if (callback) {
        callback(data);
      }
      else {
        deferred.resolve(data);
      }
    }).error(function (data) {
      deferred.reject(data);
    });
    return deferred.promise;
  }

  resource.install = function (db, username, password) {
    var deferred = $q.defer();
    if (!resource.sso) {
      delete $http.defaults.headers.common['Authorization'];
      $http.defaults.headers.common['Authorization'] = 'Basic ' + Base64.Base64.encode(username + ':' + password);
    }
    $http.post(API + 'installDatabase', db.url).success(function (data) {
      deferred.resolve(data);
    }).error(function (data) {
      deferred.reject(data);
    })
    return deferred.promise;
  }
  resource.setUrlWiki = function (urll) {
    urlWiki = urll;
  }
  resource.getUrlWiki = function () {
    return urlWiki;
  }

  resource.connect = function (database, username, password, callback, error) {
    if (!resource.sso) {
      $http.defaults.headers.common['Authorization'] = 'Basic ' + Base64.Base64.encode(username + ':' + password);
    }
    $http.get(API + 'connect/' + database).success(callback).error(error);
  }
  resource.createDatabase = function (name, type, stype, username, password, callback, error) {
    delete $http.defaults.headers.common['Authorization'];
    if (!resource.sso) {
      $http.defaults.headers.common['Authorization'] = 'Basic ' + Base64.Base64.encode(username + ':' + password);
    }
    $http.post(API + 'database/' + name + "/" + stype + "/" + type).success(function (data) {
      delete $http.defaults.headers.common['Authorization'];
      callback(data);
    }).error(function (data) {
      error(data);
    });
  }
  resource.deleteDatabase = function (name, username, password) {
    var deferred = $q.defer();
    delete $http.defaults.headers.common['Authorization'];
    if (!resource.sso) {
      $http.defaults.headers.common['Authorization'] = 'Basic ' + Base64.Base64.encode(username + ':' + password);
    }
    $http.delete(API + 'database/' + name).success(function (data) {
      deferred.resolve(data);
    }).error(function (data) {
      deferred.reject(data);
    })
    return deferred.promise;
  }
  resource.exportDatabase = function (database) {
    window.open(API + 'export/' + database);
  }
  resource.importDatabase = function (database, blob, file) {
    var fd = new FormData();
    fd.append("databaseFile", blob, file.name);
    $http.post(API + 'import/' + database, fd, {
      headers: {'Content-Type': undefined},
      transformRequest: angular.identity
    });
  }
  resource.getAllocation = function (database, callback) {
    $http.get(API + 'allocation/' + database).success(callback);
  }
  resource.getAvailableLanguages = function (database) {
    var deferred = $q.defer();

    $http.get(API + 'supportedLanguages/' + database).success(function (data) {
      deferred.resolve(data);
    }).error(function (err) {
      deferred.reject(err);
    });
    return deferred.promise;
  }
  resource.disconnect = function (callback) {
    $http.get(API + 'disconnect').success(function () {

      delete $http.defaults.headers.common['Authorization'];
      callback();
    }).error(function () {
      delete $http.defaults.headers.common['Authorization'];
      callback();
    });
  }

  return resource;
}]);
database.factory('CommandApi', ["$http", "$resource", "Notification", "Spinner", "$q", function ($http, $resource, Notification, Spinner, $q) {

  var resource = $resource(API + 'command/:database');

  resource.queryText = function (params, callback, error) {
    var startTime = new Date().getTime();
    var limit = params.limit || 20;
    var verbose = params.verbose != undefined ? params.verbose : true;
    var shallow = ''; //disabled shallow params.shallow ? ',shallow' :
    var contentType = params.contentType || 'application/json';
    //rid,type,version,class,attribSameRow,indent:2,dateAsLong,shalow,graph
    var text = API + 'command/' + params.database + "/" + params.language + "/-/" + limit + '?format=rid,type,version' + shallow + ',class,graph';


    if (contentType == 'text/csv') {
      var query = params.text.trim();
      var config = {headers: {"accept": contentType}};
      $http.post(text, query, config).success(function (data) {
        var time = ((new Date().getTime() - startTime) / 1000);
        var records = data.result ? data.result.length : "";
        var form = $('<a style="display: none;" type="hidden" id="linkdownload" href="data:application/octet-stream;charset=utf-8;base64,' + Base64.encode(data) + '">asdasdasasd</a>');
        $('#download').append(form);


        query = query.replace(/ /gi, '');
        query = query.replace(/[^\w\s]/gi, '');
        query = query.replace(/^\*/gi, '');
        var namefile = query.substring(0, 15);
        document.getElementById('linkdownload').setAttribute("download", namefile + ".csv");
        document.getElementById('linkdownload').click();
        var elem = document.getElementById('linkdownload');
        elem.parentNode.removeChild(elem);
        var noti = "Query executed  in " + time + " sec. Returned " + records + " record(s)";
        if (verbose) {
          Notification.push({content: noti});

        }
        callback(data);
      }).error(function (data) {
        Notification.push({content: data});
        if (error) error(data);
      });
      ;

    }

    else {
      if (params.text) {

        var query = params.text.trim();
        var config = {headers: {"accept": contentType}};
        $http.post(text, query, config).success(function (data) {

          var time = ((new Date().getTime() - startTime) / 1000);
          var records = data.result ? data.result.length : "";

          var noti = "Query executed in " + time + " sec. Returned " + records + " record(s)";
          if (verbose) {
            Notification.push({content: noti});
          }

          if (data != undefined) {
            data.notification = noti;
            callback(data);
          }
          else {
            callback('ok');
          }

        }).error(function (data) {
          if (verbose) {
            Notification.push({content: data, error: true});
          }
          if (!verbose && !error) {
            Notification.push({content: data, error: true});
          }
          if (error) error(data);
        });
      }
    }
  }


  resource.graphQuery = function (query, params) {


    var deferred = $q.defer();

    var limit = params.limit || 20;

    var text = API + 'command/' + params.database + "/" + params.language + "/-/" + limit;

    var opts = {
      command: query,
      mode: "graph"
    }
    $http.post(text, opts).success(deferred.resolve).error(deferred.reject);
    return deferred.promise;

  }
  resource.getAll = function (database, clazz, callback) {
    var text = API + 'command/' + database + '/sql/-/-1?format=rid,type,version,class,shallow,graph';
    var query = "select * from " + clazz;
    $http.post(text, query).success(function (data) {
      callback(data);
    });

  }
  resource.interrupt = function (database, command) {
    var deferred = $q.defer();
    var text = API + 'dbconnection/' + database;
    $http.post(text, command).success(function (data) {
      deferred.resolve(data);
    });
    return deferred.promise;
  }
  return resource;
}]);

database.factory('BatchApi', ["$http", "$resource", "Notification", "Spinner", "$q", function ($http, $resource, Notification, Spinner, $q) {


  var resource = $resource(API + 'command/:database');

  resource.executeBatch = function (database, script, language) {

    var deferred = $q.defer();

    var url = API + 'batch/' + database;

    var cmd = {
      type: "script",
      language: language,
      script: script

    }

    var operations = {
      operations: [cmd]
    }
    var startTime = new Date().getTime();
    $http.post(url, operations).success(function (data) {

      if (data && data.result) {

        var time = ((new Date().getTime() - startTime) / 1000);
        var records = data.result ? data.result.length : "";
        var noti = "Query executed  in " + time + " sec. Returned " + records + " record(s)";
        data.notification = noti;
      }

      deferred.resolve(data);
    }).error(function (data) {
      deferred.reject(data);
    })
    return deferred.promise;
  }
  return resource;
}]);
database.factory('DocumentApi', ["$http", "$resource", "Database", "$q", function ($http, $resource, Database, $q) {

  var resource = $resource(API + 'document/:database/:document');
  resource.updateDocument = function (database, rid, doc, callback) {
    var deferred = $q.defer()
    $http.put(API + 'document/' + database + "/" + rid.replace('#', ''), doc).success(function (data) {
      if (callback) {
        callback(data)
      }
      deferred.resolve(data);
    }).error(function (data) {
      if (callback) {
        callback(data)
      }
      deferred.reject(data);
    });
    return deferred.promise;

  }
  resource.uploadFileDocument = function (database, doc, blob, name, callback) {

    var fd = new FormData();
    fd.append("linkValue", JSON.stringify(doc));
    fd.append("file", blob, name);
    //$.post(API + 'uploadSingleFile/'+database,fd);
    $http.post(API + 'uploadSingleFile/' + database, fd, {
      headers: {'Content-Type': undefined},
      transformRequest: angular.identity
    });
    //$http.put(API + 'document/' + database + "/" + rid.replace('#',''),doc,{headers: { 'Content-Type': undefined }}).success(callback).error(callback);
  }
  resource.createDocument = function (database, rid, doc, callback) {
    var deferred = $q.defer()
    $http.post(API + 'document/' + database + "/" + rid.replace('#', ''), doc).success(function (data) {

      if (callback) {
        callback(data)
      }
      deferred.resolve(data);
    }).error(function (data) {
      if (callback) {
        callback(data)
      }
      deferred.reject(data);
    });
    return deferred.promise;
  }
  resource.deleteDocument = function (database, rid, callback) {
    var deferred = $q.defer()
    $http.delete(API + 'document/' + database + "/" + rid.replace('#', '')).success(function (data) {
      if (callback) {
        callback(data);
      }
      deferred.resolve(data);
    }).error(function (data) {
      if (callback) {
        callback(data);
      }
      deferred.reject(data);
    });
    return deferred.promise;
  }
  resource.isNew = function (doc) {
    return doc['@rid'] == '#-1:-1';
  }
  resource.createNewDoc = function (clazz) {
    var r = new resource
    var fields = Database.listField(clazz);
    r['@class'] = clazz;
    r['@version'] = 0;
    r['@rid'] = '#-1:-1';
    for (var i = 0; i < fields.length; i++) {
      r[fields[i]] = null;
    }
    ;
    return r;
  }
  return resource;
}]);
database.factory('ServerApi', ["$http", "$resource", "$q", function ($http, $resource, $q) {


  var resource = $resource(API + 'server');
  resource.getServerInfo = function (callback) {

    $http.get(API + 'server').success(function (data) {
      callback(data);
    });
  }
  resource.killConnection = function (n, callback) {
    $http.post(API + 'connection/kill/' + n).success(function () {
      callback();
    });
  }
  resource.interruptConnection = function (n, callback) {
    $http.post(API + 'connection/interrupt/' + n).success(function () {
      callback();
    });
  }

  resource.changeConfiguration = function (name, val) {
    var deferred = $q.defer();
    var text = API + 'server/configuration.' + name + '/' + val;
    $http.post(text).success(function (data) {
      deferred.resolve(data)
    }).error(function (data) {
      deferred.reject(data);
    });
    return deferred.promise;
  }
  return resource;
}]);

database.factory('FunctionApi', ["$http", "$resource", "Notification", function ($http, $resource, Notification) {


  var resource = $resource('function/:database');
  resource.executeFunction = function (params, callback, error) {
    var startTime = new Date().getTime();
    var verbose = params.verbose != undefined ? params.verbose : true;
    if (params.parameters == '') {
      var text = API + 'function/' + params.database + "/" + params.functionName;
    }
    else {
      var text = API + 'function/' + params.database + "/" + params.functionName + '/' + params.parameters;
    }


    $http.post(text).success(function (data) {
      var time = ((new Date().getTime() - startTime) / 1000);
      var records = data.result ? data.result.length : "";
      if (verbose) {
        var noti = "Function " + params.functionName + " executed in " + time + " sec. ";
        Notification.push({content: noti, autoHide: true});
      }
      callback(data);
    }).error(function (data) {
      if (verbose) {
        Notification.push({
          content: "Error executing function " + params.functionName + ".",
          error: true,
          autoHide: true
        });
      }
      if (error) error(data);
    });
  }
  return resource;
}]);

database.factory('DatabaseAlterApi', ["$http", "$resource", "$q", function ($http, $resource, $q) {


  var resource = $resource('function/:database');


  resource.changeProperty = function (database, props) {

    var deferred = $q.defer();
    var text = API + 'command/' + database + '/sql/-/-1?format=rid,type,version,class,graph';
    var query = "alter database {{name}} {{value}}"
    var queryText = S(query).template(props).s;
    $http.post(text, queryText).success(function (data) {
      deferred.resolve(data)
    }).error(function (data) {
      deferred.reject(data);
    });
    return deferred.promise;
  }
  resource.changeCustomProperty = function (database, props) {

    var deferred = $q.defer();
    var text = API + 'command/' + database + '/sql/-/-1?format=rid,type,version,class,graph';
    var query = "alter database custom {{name}} = {{value}}"
    var queryText = S(query).template(props).s;
    $http.post(text, queryText).success(function (data) {
      deferred.resolve(data)
    }).error(function (data) {
      deferred.reject(data);
    });
    return deferred.promise;
  }
  return resource
}]);
database.factory('ClassAlterApi', ["$http", "$resource", "$q", function ($http, $resource, $q) {


  var resource = $resource('function/:database');

  resource.changeProperty = function (database, props) {

    var deferred = $q.defer();
    var text = API + 'command/' + database + '/sql/-/-1?format=rid,type,version,class,graph';
    var query = "alter class {{clazz}} {{name}} {{value}}"
    var queryText = S(query).template(props).s;
    $http.post(text, queryText).success(function (data) {
      deferred.resolve(data)
    }).error(function (data) {
      deferred.reject(data);
    });
    return deferred.promise;
  }
  return resource
}]);
database.factory('PropertyAlterApi', ["$http", "$resource", "$q", function ($http, $resource, $q) {


  var resource = $resource('function/:database');


  resource.changeProperty = function (database, props) {

    var deferred = $q.defer();
    var text = API + 'command/' + database + '/sql/-/-1?format=rid,type,version,class,graph';
    var query = "alter property {{clazz}}.{{property}} {{name}} {{value}}"
    var queryText = S(query).template(props).s;
    $http.post(text, queryText).success(function (data) {
      deferred.resolve(data)
    }).error(function (data) {
      deferred.reject(data);
    });
    return deferred.promise;
  }
  return resource
}]);
database.factory('ClusterAlterApi', ["$http", "$resource", "$q", function ($http, $resource, $q) {


  var resource = $resource('');


  resource.changeProperty = function (database, props) {

    var deferred = $q.defer();
    var text = API + 'command/' + database + '/sql/-/-1?format=rid,type,version,class,graph';
    var query = "alter cluster {{cluster}} {{name}} {{value}}"
    var queryText = S(query).template(props).s;
    $http.post(text, queryText).success(function (data) {
      deferred.resolve(data)
    }).error(function (data) {
      deferred.reject(data);
    });
    return deferred.promise;
  }
  return resource
}]);


export default  database.name;
