var GrapgController = angular.module('vertex.controller', []);
GrapgController.controller("VertexCreateController", ['$scope', '$routeParams', '$location', 'DocumentApi', 'Database', 'Notification', function ($scope, $routeParams, $location, DocumentApi, Database, Notification) {


  var database = $routeParams.database;
  var clazz = $routeParams.clazz
  $scope.fixed = Database.header;
  $scope.doc = DocumentApi.createNewDoc(clazz);
  $scope.headers = Database.getPropertyFromDoc($scope.doc);
  $scope.save = function () {
    DocumentApi.createDocument(database, $scope.doc['@rid'], $scope.doc, function (data) {
      Notification.push({content: JSON.stringify(data)});
      $location.path('/database/' + database + '/browse/edit/' + data['@rid'].replace('#', ''));
    });

  }
}]);
GrapgController.controller("VertexModalController", ['$scope', '$routeParams', '$location', 'DocumentApi', 'Database', 'Notification', function ($scope, $routeParams, $location, DocumentApi, Database, Notification) {


  $scope.reload = function () {
    $scope.doc = DocumentApi.get({database: $scope.db, document: $scope.rid}, function () {
      $scope.headers = Database.getPropertyFromDoc($scope.doc);
    }, function (error) {
      Notification.push({content: JSON.stringify(error)});
      $location.path('#/404');
    });
  }
  $scope.save = function () {
    DocumentApi.updateDocument($scope.db, $scope.rid, $scope.doc, function (data) {
      Notification.push({content: data});
    });

  }
  $scope.reload();
}]);
GrapgController.controller("VertexEditController", ['$scope', '$injector', '$routeParams', '$location', '$modal', '$q', 'DocumentApi', 'Database', 'CommandApi', 'Notification', function ($scope, $injector, $routeParams, $location, $modal, $q, DocumentApi, Database, CommandApi, Notification) {

  $injector.invoke(BaseEditController, this, {$scope: $scope});
  Database.setWiki("Edit-vertex.html");
  $scope.label = 'Vertex';
  $scope.fixed = Database.header;
  $scope.canSave = true;
  $scope.canDelete = true;
  $scope.canCreate = true;
  $scope.canAdd = true;
  $scope.pageLimit = 10;
  $scope.limitProof = 10;
  $scope.limits = [];
  $scope.popover = {
    title: 'Add edge'
  }

  // Toggle modal
  $scope.showModal = function (rid) {
    var modalScope = $scope.$new(true);
    modalScope.db = $scope.database;
    modalScope.rid = rid;
    var modalPromise = $modal({
      template: 'views/database/modalEdit.html',
      persist: false,
      show: false,
      scope: modalScope,
      modalClass: 'editEdge'
    });
    modalPromise.$promise.then(modalPromise.show);

  };
  $scope.showModalConnection = function (label) {
    var modalScope = $scope.$new(true);
    modalScope.db = $scope.database;
    modalScope.originRid = $scope.rid;
    modalScope.container = $scope;
    modalScope.label = label
    var modalPromise = $modal({
      template: 'views/vertex/modalConnection.html',
      persist: false,
      show: false,
      scope: modalScope,
      modalClass: 'createEdge'
    });
    modalPromise.$promise.then(modalPromise.show);

  }
  $scope.initLimits = function () {
    $scope.incomings.forEach(function (i) {
      $scope.limits[i] = $scope.pageLimit;
    })
    $scope.outgoings.forEach(function (i) {
      $scope.limits[i] = $scope.pageLimit;
    })
  }
  if (!$scope.doc) {
    $scope.reload();
  } else {
    $scope.headers = Database.getPropertyFromDoc($scope.doc);
    $scope.isGraph = Database.isGraph($scope.doc['@class']);
    $scope.incomings = Database.getEdge($scope.doc, 'in_');
    $scope.outgoings = Database.getEdge($scope.doc, 'out_');
    $scope.exclude = $scope.outgoings.concat($scope.incomings);
    $scope.outgoings = $scope.outgoings.concat((Database.getLink($scope.doc, $scope.exclude)));

    $scope.label = Database.isEdge($scope.doc['@class']) ? "Edge" : "Vertex";

    $scope.initLimits();
  }


  $scope.more = function (i) {
    $scope.limits[i] += $scope.pageLimit;
  }
  $scope.less = function (i) {
    $scope.limits[i] -= $scope.pageLimit;
  }
  $scope.isHideMore = function (i) {
    return $scope.limits[i] >= $scope.doc[i].length;
  }
  $scope.isHideLess = function (i) {
    return $scope.limits[i] == $scope.pageLimit;
  }
  $scope.delete = function () {
    var recordID = $scope.doc['@rid'];
    var label = Database.isEdge($scope.doc['@class']) ? "Edge" : "Vertex";
    Utilities.confirm($scope, $modal, $q, {
      title: 'Warning!',
      body: 'You are removing ' + label + ' ' + recordID + '. Are you sure?',
      success: function () {
        var command = "DELETE " + label + ' ' + recordID;
        CommandApi.queryText({database: $scope.database, language: 'sql', text: command}, function (data) {
          var clazz = $scope.doc['@class'];
          $location.path('/database/' + $scope.database + '/browse/' + 'select * from ' + clazz);
        });
      }
    });
  }

  $scope.filterArray = function (arr, i) {
    if (!$scope.limits[i]) {
      $scope.limits[i] = {};
    }
    if (arr instanceof Array) {
      return arr;
    } else {
      var newArr = new Array;
      newArr.push(arr);
      return newArr;
    }

  }
  $scope.follow = function (rid) {
    var edgeDoc = DocumentApi.get({database: $scope.database, document: rid}, function () {
      if (Database.isEdge(edgeDoc['@class'])) {
        $scope.showModal(rid);
      }
      else {
        $scope.navigate(rid);
      }

    }, function (error) {
      Notification.push({content: JSON.stringify(error)});
      $location.path('/404');
    });

  }
  $scope.followEdge = function (rid, direction) {
    var edgeDoc = DocumentApi.get({database: $scope.database, document: rid}, function () {
      var ridNavigate = rid;
      if (Database.isEdge(edgeDoc['@class'])) {
        ridNavigate = edgeDoc[direction];
      }
      $scope.navigate(ridNavigate);
    }, function (error) {
      Notification.push({content: JSON.stringify(error)});
      $location.path('/404');
    });

  }
  $scope.deleteLink = function (group, edge) {

    Utilities.confirm($scope, $modal, $q, {
      title: 'Warning!',
      body: 'You are removing edge ' + edge + '. Are you sure?',
      success: function () {

        var edgeDoc = DocumentApi.get({database: $scope.database, document: edge}, function () {
          var command = ""
          if (Database.isEdge(edgeDoc['@class'])) {
            command = "DELETE EDGE " + edge;
          }
          else {
            if (group.contains('in_')) {
              command = "DELETE EDGE FROM " + edge + " TO " + $scope.rid + " where @class='" + group.replace("in_", "") + "'";
            } else {
              command = "DELETE EDGE FROM " + $scope.rid + " TO " + edge + " where @class='" + group.replace("out_", "") + "'";
            }
          }
          CommandApi.queryText({database: $scope.database, language: 'sql', text: command}, function (data) {
            $scope.reload();
          });
        }, function (error) {
          Notification.push({content: JSON.stringify(error)});
          $location.path('/404');
        });

      }
    });
  }
}]);
GrapgController.controller("VertexPopoverLabelController", ['$scope', '$routeParams', '$location', 'DocumentApi', 'Database', 'Notification', function ($scope, $routeParams, $location, DocumentApi, Database, Notification) {

  $scope.init = function (where) {
    $scope.where = where;
    $scope.labels = Database.getClazzEdge();
  }
  $scope.addEdgeLabel = function () {
    var name = "";
    if ($scope.where == "outgoings") {
      name = "out_".concat($scope.popover.name);
    }
    else {
      name = "in_".concat($scope.popover.name);
    }
    if ($scope[$scope.where].indexOf(name) == -1)
      $scope[$scope.where].push(name);
    delete $scope.popover.name;
  }

}]);

GrapgController.controller("VertexModalBrowseController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', 'Icon', '$timeout', function ($scope, $routeParams, $location, Database, CommandApi, Icon, $timeout) {

  $scope.database = Database;
  $scope.limit = 20;
  $scope.queries = new Array;
  $scope.added = new Array;
  $scope.loaded = true;


  $scope.editorOptions = {
    lineWrapping: true,
    lineNumbers: true,
    readOnly: false,
    mode: 'text/x-sql',
    metadata: Database,
    extraKeys: {
      "Ctrl-Enter": function (instance) {
        $scope.$apply(function () {
          $scope.query();
        });
      },
      "Ctrl-Space": "autocomplete"
    },
    onLoad: function (_cm) {
      $scope.cm = _cm;

      $scope.cm.on("change", function () { /* script */
        var wrap = $scope.cm.getWrapperElement();
        var approp = $scope.cm.getScrollInfo().height > 300 ? "300px" : "auto";
        if (wrap.style.height != approp) {
          wrap.style.height = approp;
          $scope.cm.refresh();
        }
      });
      $scope.cm.refresh();

    }
  };
  $scope.query = function () {

    CommandApi.queryText({
      database: $routeParams.database,
      language: 'sql',
      text: $scope.queryText,
      limit: $scope.limit,
      verbose: false
    }, function (data) {
      if (data.result) {
        $scope.headers = Database.getPropertyTableFromResults(data.result);
        $scope.results = data.result;
      }
      if ($scope.queries.indexOf($scope.queryText) == -1)
        $scope.queries.push($scope.queryText);
    }, function err(data) {
      $scope.error = data;
      $timeout(function () {
        $scope.error = null;
      }, 2000);
    });
  }


  $scope.select = function (result) {
    var index = $scope.added.indexOf(result['@rid']);
    if (index == -1) {
      $scope.added.push(result['@rid']);
    } else {
      $scope.added.splice(index, 1);
    }
  }
  $scope.createEdges = function () {

    var command;
    if ($scope.label.contains('in_')) {
      command = "CREATE EDGE " + $scope.label.replace("in_", "") + " FROM [" + $scope.added + "]" + " TO " + $scope.originRid;
    } else {
      command = "CREATE EDGE " + $scope.label.replace("out_", "") + " FROM " + $scope.originRid + " TO [" + $scope.added + "]";
    }
    CommandApi.queryText({database: $routeParams.database, language: 'sql', text: command}, function (data) {
      $scope.added = new Array;
      $scope.container.reload();
    });


  }


}]);

GrapgController.controller("GraphController", ['$scope', '$routeParams', '$location', '$modal', '$q', 'Database', 'CommandApi', 'Spinner', 'Aside', 'DocumentApi', 'localStorageService', 'Graph', 'Icon', 'GraphConfig', 'Notification', '$rootScope', 'History', '$timeout','scroller', function ($scope, $routeParams, $location, $modal, $q, Database, CommandApi, Spinner, Aside, DocumentApi, localStorageService, Graph, Icon, GraphConfig, Notification, $rootScope, History, $timeout,scroller) {

  var data = [];
  $scope.currentIndex = -1;

  $scope.history = History.histories();

  $scope.fullscreen = false;
  $scope.additionalClass = '';
  $scope.database = Database;
  Database.setWiki("Graph-Editor.html")
  $scope.dirty = false;
  $rootScope.$on('graphConfig:changed', function (val) {
    $scope.dirty = val;
  })
  $rootScope.$on('graphConfig:onSave', function (val) {
    $scope.saveConfig();
  })

  $rootScope.$on("aside:close", function () {
    $scope.listClass = 'fa-mail-forward';
  })
  $rootScope.$on("aside:open", function () {

    $scope.listClass = 'fa-mail-reply';
  })


  $scope.toggleProperties = function () {

    Aside.toggle();


    if (Aside.isOpen()) {
      $scope.graphClass = "svg-container-collapsed";
    } else {
      $scope.graphClass = "svg-container-expanded";
    }

  }

  $scope.graphClass = "svg-container-collapsed";
  Aside.show({
    scope: $scope,
    title: "Vertex/Edge Panel",
    template: 'views/database/graph/asideEmpty.html',
    show: true,
    absolute: false,
    fullscreen: $scope.fullscreen
  });


  $scope.$watch("fullscreen", function (val) {
    if (val) {
      $scope.additionalClass = 'panel-graph-fullscreen';
      $scope.graph.fullScreen(true);
      Aside.fullScreen(true);
    } else {
      if ($scope.graph) {
        $scope.graph.fullScreen(false);
      }
      Aside.fullScreen(false);
      $scope.additionalClass = '';
    }
  })
  $scope.editorOptions = {
    lineWrapping: true,
    lineNumbers: true,
    readOnly: false,
    mode: 'text/x-sql',
    metadata: Database,
    extraKeys: {
      "Ctrl-Enter": function (instance) {
        $scope.$apply(function () {
          if ($scope.queryText)
            $scope.query();
        });

      },
      "Ctrl-Space": "autocomplete",
      'Cmd-/': 'toggleComment',
      'Ctrl-/': 'toggleComment',
      "Cmd-Up": function (instance) {


        if ($scope.currentIndex < $scope.history.length - 1) {

          var tmp = $scope.queryText;
          if ($scope.currentIndex == -1) {
            $scope.prevText = tmp;
          }
          $scope.currentIndex++;
          $scope.queryText = $scope.history[$scope.currentIndex];
          $scope.$apply();
        }


      },
      "Cmd-Down": function (instance) {
        if ($scope.currentIndex >= 0) {

          $scope.currentIndex--;

          if ($scope.currentIndex == -1) {
            $scope.queryText = $scope.prevText
          } else {
            $scope.queryText = $scope.history[$scope.currentIndex];
          }

          $scope.$apply();
        }
      }

    },
    onLoad: function (_cm) {
      $scope.cm = _cm;
      if ($routeParams.q) {
        $scope.queryText = $routeParams.q;
        $scope.query();
      }
      $scope.cm.on("change", function () { /* script */
        var wrap = $scope.cm.getWrapperElement();
        var approp = $scope.cm.getScrollInfo().height > 300 ? "300px" : "auto";
        if (wrap.style.height != approp) {
          wrap.style.height = approp;
          $scope.cm.refresh();
        }
      });
      $scope.cm.focus();
      scroller.scrollTo(0, 0, 0);
    }
  };
  if (Database.hasClass(GraphConfig.CLAZZ)) {

    GraphConfig.get().then(function (data) {
      if (!data) {
        var newCfg = DocumentApi.createNewDoc(GraphConfig.CLAZZ);
        GraphConfig.set(newCfg).then(function (data) {
          $scope.gConfig = data;
        })
      } else {
        $scope.gConfig = data;
      }

    });
  } else {

    GraphConfig.init().then(function () {
      var newCfg = DocumentApi.createNewDoc(GraphConfig.CLAZZ);
      GraphConfig.set(newCfg).then(function (data) {
        $scope.gConfig = data;
      })
    });
  }

  $scope.$watch('gConfig', function (data) {
    if (data) {
      $scope.tmpGraphOptions.config = data.config;
      $scope.graphOptions = $scope.tmpGraphOptions;
    }
    //
  })


  var config = {
    height: 500,
    width: 1200,
    classes: {},
    node: {
      r: 30
    }

  }


  $scope.resetZoom = function () {
    $scope.graph.resetZoom();
  }

  $scope.download = function () {

    var html = d3.select(".graph-container svg")
      .attr("version", 1.1)
      .attr("xmlns", "http://www.w3.org/2000/svg")
      .node().parentNode.innerHTML;


    var blob = new Blob([html], {type: "image/svg+xml"});
    saveAs(blob, "myProfile.svg");


  }
  $scope.clear = function () {
    $scope.graph.clear();
    Graph.data = [];
  }
  $scope.queryText = Graph.query;
  $scope.tmpGraphOptions = {
    data: Graph.data,
    onLoad: function (graph) {
      $scope.graph = graph;

      $scope.graph.on('node/click', function (v) {

        var q = "SELECT outE().@class.asSet() as out, inE().@class.asSet() as in from " + v.source["@rid"];
        CommandApi.queryText({
          database: $routeParams.database,
          contentType: 'JSON',
          language: 'sql',
          text: q,
          limit: -1,
          shallow: false,
          verbose: false
        }, function (data) {
          v.relationships = data.result[0];
        })
        if (Aside.isOpen()) {
          $timeout(function () {
            $scope.doc = v.source;
            var title = $scope.doc['@class'] + "-" + $scope.doc['@rid'];
            Aside.show({
              scope: $scope,
              title: title,
              template: 'views/database/graph/asideVertex.html',
              show: true,
              absolute:false,
              fullscreen: $scope.fullscreen
            });
          }, 200);
        }
      });
      $scope.graph.on('edge/create', function (v1, v2) {

        if (v2['@rid'].startsWith("#-")) {
          $scope.$apply(function () {
            Notification.push({
              content: 'Cannot connect ' + v1['@rid'] + ' to a temporary node',
              autoHide: true,
              warning: true
            });
            $scope.graph.endEdgeCreation();
          });
        } else {
          $scope.showModalNewEdge(v1, v2);
        }
      });

      function openEdge(scope, e) {
        scope.doc = e.edge;
        var title = "Edge (" + e.label + ")";
        Aside.show({
          scope: scope,
          title: title,
          template: 'views/database/graph/asideEdge.html',
          show: true,
          absolute:false,
          fullscreen: scope.fullscreen
        });
      }

      $scope.graph.on('edge/click', function (e) {


        if (typeof e.edge == 'string') {
          DocumentApi.get({database: $routeParams.database, document: e.edge}, function (doc) {
            e.edge = doc;
            if (Aside.isOpen()) {
              openEdge($scope, e);
            }
          });
        }
        if (Aside.isOpen()) {
          openEdge($scope, e);
        }


      });
      $scope.graph.on('node/dblclick', function (v) {

        if (v['@rid'].startsWith("#-")) {

          $scope.$apply(function () {
            Notification.push({content: 'Cannot expand a temporary node', autoHide: true, warning: true});
          });

        } else {

          var q = "traverse both() from " + v['@rid'] + " while $depth < 2"
          CommandApi.graphQuery(q, {
            database: $routeParams.database,
            language: 'sql',
            limit: -1
          }).then(function (data) {
            $scope.graph.data(data.graph).redraw();
          })
        }
      });

      $scope.graph.on('node/load', function (v, callback) {
        DocumentApi.get({database: $routeParams.database, document: v.source}, function (doc) {
          callback(doc);
        });
      });
    },
    metadata: Database.getMetadata(),
    config: config,
    edgeMenu: [
      {
        name: '\uf044',
        onClick: function (e) {
          if (e.edge) {
            $scope.showModal(e, e.edge["@rid"]);
          }
        }


      },
      {
        name: '\uf06e',
        onClick: function (e) {

          var title = "Edge (" + e.label + ")";
          $scope.doc = e.edge;
          Aside.show({
            scope: $scope,
            title: title,
            template: 'views/database/graph/asideEdge.html',
            show: true,
            absolute: false,
            fullscreen: $scope.fullscreen
          });
        }
      },
      {
        name: '\uf127',
        onClick: function (e) {

          var recordID = e['@rid']
          Utilities.confirm($scope, $modal, $q, {
            title: 'Warning!',
            body: 'You are removing Edge ' + e.label + ' from ' + e.source["@rid"] + ' to ' + e.target["@rid"] + ' . Are you sure?',
            success: function () {


              var command = ""
              if (e.edge && e.edge["@rid"]) {
                command = "DELETE EDGE " + e.edge["@rid"];
              }
              else {
                command = "DELETE EDGE " + e.label + " FROM " + e.source["@rid"] + " TO " + e.target["@rid"] + " where @class='" + e.label + "'";
              }

              CommandApi.queryText({
                database: $routeParams.database,
                language: 'sql',
                text: command,
                verbose: false
              }, function (data) {
                $scope.graph.removeEdge(e);
              });
            }
          });

        }
      }
    ],
    menu: [
      {
        name: '\uf044',
        onClick: function (v) {
          if (v['@rid'].startsWith("#-")) {

            $scope.$apply(function () {
              Notification.push({content: 'Cannot edit a temporary node', autoHide: true, warning: true});
            });

          } else {
            $scope.showModal(v, v.source["@rid"]);
          }
        }
      },

      {
        name: "\uf18e",
        onClick: function (v) {

        },
        submenu: {
          type: "tree",
          entries: function (v) {


            var acts = [];
            if (v.relationships && v.relationships.out) {
              v.relationships.out.forEach(function (elem) {
                var name = elem.replace("out_", "");
                acts.push(
                  {
                    name: (name != "" ? name : "E"),
                    onClick: function (v, label) {

                      if (v['@rid'].startsWith("#-")) {

                        $scope.$apply(function () {
                          Notification.push({
                            content: 'Cannot navigate relationship of a temporary node',
                            autoHide: true,
                            warning: true
                          });
                        });

                      } else {
                        if (label == "E") {
                          label = "";
                        }
                        else {
                          label = "'" + label + "'";
                        }

                        var props = {rid: v['@rid'], label: label};
                        var query = "traverse out({{label}})  from {{rid}} while $depth < 2 "
                        var queryText = S(query).template(props).s;


                        CommandApi.graphQuery(queryText, {
                          database: $routeParams.database,
                          language: 'sql',
                          limit: -1
                        }).then(function (data) {
                          $scope.graph.data(data.graph).redraw();
                        })

                      }
                    }
                  }
                )

              })
            }
            return acts;
          }

        }

      },
      {
        name: "...",
        onClick: function (v) {

        },
        submenu: {
          type: "pie",
          entries: [
            {
              name: "\uf014",
              placeholder: "Delete",
              onClick: function (v, label) {

                if (v['@rid'].startsWith("#-")) {

                  $scope.$apply(function () {
                    Notification.push({content: 'Cannot delete a temporary node', autoHide: true, warning: true});
                  });

                } else {
                  var recordID = v['@rid']
                  Utilities.confirm($scope, $modal, $q, {
                    title: 'Warning!',
                    body: 'You are removing Vertex ' + recordID + '. Are you sure?',
                    success: function () {
                      var command = "DELETE Vertex " + recordID;
                      CommandApi.queryText({
                        database: $routeParams.database,
                        language: 'sql',
                        text: command,
                        verbose: false
                      }, function (data) {
                        $scope.graph.removeVertex(v);
                      });
                    }
                  });
                }
              }
            },
            {
              name: "\uf12d",
              placeholder: "Remove from canvas",
              onClick: function (v, label) {
                $scope.graph.removeVertex(v);

              }
            }

          ]
        }
      },
      {
        name: "\uf0c1",
        placeholder: "Connect",
        onClick: function (v) {

          if (v['@rid'].startsWith("#-")) {

            $scope.$apply(function () {
              Notification.push({content: 'Cannot connect a temporary node', autoHide: true, warning: true});
            });

          } else {

            $scope.graph.startEdge();
          }
        }
      },
      {
        name: "\uf18e",
        onClick: function (v) {

        },
        submenu: {
          type: "tree",
          entries: function (v) {

            var acts = [];
            if (v.relationships || v.relationships.in) {
              //var outgoings = Database.getEdge(v.source, 'in_');
              v.relationships.in.forEach(function (elem) {
                var name = elem.replace("in_", "");

                acts.push(
                  {
                    name: (name != "" ? name : "E"),
                    onClick: function (v, label) {
                      if (label == "E") {
                        label = "";
                      }
                      else {
                        label = "'" + label + "'";
                      }

                      var props = {rid: v['@rid'], label: label};
                      var query = "traverse in({{label}})  from {{rid}} while $depth < 2 "
                      var queryText = S(query).template(props).s;


                      CommandApi.graphQuery(queryText, {
                        database: $routeParams.database,
                        language: 'sql',
                        limit: -1
                      }).then(function (data) {
                        $scope.graph.data(data.graph).redraw();
                      })
                      //CommandApi.queryText({
                      //  database: $routeParams.database,
                      //  contentType: 'JSON',
                      //  language: 'sql',
                      //  text: queryText,
                      //  limit: -1,
                      //  shallow: false,
                      //  verbose: false
                      //}, function (data) {
                      //
                      //  $scope.graph.data(data.result).redraw();
                      //})
                    }
                  }
                )
              })
            }
            return acts;
          }

        }
      },
      {
        name: "\uf06e",
        onClick: function (v) {
          $scope.doc = v.source;
          var title = $scope.doc['@class'] + " - " + $scope.doc['@rid']
          Aside.show({
            scope: $scope,
            title: title,
            template: 'views/database/graph/asideVertex.html',
            show: true,
            absolute: false,
            fullscreen: $scope.fullscreen
          });

        }
      }
    ]


  }
  $scope.toggleQuery = function () {
    $scope.queryClass = !$scope.queryClass;
  }
  $scope.hideLegend = function () {
    $scope.graph.toggleLegend();
  }
  $scope.showModalNewEdge = function (source, target) {
    var modalScope = $scope.$new(true);
    modalScope.db = $routeParams.database;
    modalScope.database = $routeParams.database;
    modalScope.isNew = true;
    modalScope.source = source;
    modalScope.target = target;
    modalScope.confirmSave = function (docs) {
      $scope.graph.endEdgeCreation();
      $scope.graph.data({edges: docs}).redraw();
    }
    modalScope.cancelSave = function (error) {

      $scope.graph.endEdgeCreation();
      if (error) {
        Notification.push({content: error, error: true, autoHide: true});
      }
    }
    var modalPromise = $modal({
      template: 'views/database/modalNewEdge.html',
      persist: false,
      show: false,
      scope: modalScope,
      modalClass: 'editEdge'
    });


    modalPromise.$promise.then(modalPromise.show);


  };
  $scope.showModalNew = function () {
    var modalScope = $scope.$new(true);
    modalScope.db = $routeParams.database;
    modalScope.database = $routeParams.database;
    modalScope.isNew = true;
    modalScope.confirmSave = function (doc) {

      $scope.graph.data({vertices: [doc]}).redraw();
    }
    $modal({
      template: 'views/database/modalNew.html',
      persist: false,
      show: true,
      scope: modalScope,
      modalClass: 'editEdge'
    });

  };
  $scope.addNode = function () {
    $scope.showModalNew();
  }
  $scope.showModal = function (v, rid) {
    var modalScope = $scope.$new(true);
    modalScope.db = $routeParams.database;
    modalScope.database = $routeParams.database;
    modalScope.rid = rid;
    modalScope.confirmSave = function (doc) {
      if (v.edge) {
        v.edge = doc;
      } else if (v.source) {
        v.source = doc;
        $scope.doc = v.source;
        var title = $scope.doc['@class'] + "-" + $scope.doc['@rid'] + "- Version " + $scope.doc['@version'];
        Aside.show({
          scope: $scope,
          title: title,
          template: 'views/database/graph/asideVertex.html',
          show: true,
          absolute: false,
          fullscreen: $scope.fullscreen
        });
      }
    }
    $modal({
      template: 'views/database/modalEdit.html',
      persist: false,
      show: true,
      scope: modalScope,
      modalClass: 'editEdge'
    });

  };
  $scope.saveConfig = function () {
    $scope.gConfig.config = $scope.graph.getConfig();
    GraphConfig.set($scope.gConfig).then(function (data) {
      $scope.gConfig = data;
      Notification.push({content: 'Configuration Saved Correctly', autoHide: true});
    });
  }
  $scope.query = function () {

    Spinner.start();
    if ($scope.queryText.startsWith('g.')) {
      $scope.language = 'gremlin';
    } else {
      $scope.language = 'sql';
    }

    var queryBuffer = $scope.queryText;
    var selection = $scope.cm.getSelection();
    if (selection && selection != "") {
      queryBuffer = "" + selection;
    }
    CommandApi.graphQuery(queryBuffer, {
      database: $routeParams.database,
      language: $scope.language,
    }).then(function (data) {
      $scope.graph.data(data.graph).redraw();
      $scope.history = History.push(queryBuffer);
      $scope.currentIndex = -1;
      Spinner.stopSpinner();
    }).catch(function (err) {
      Spinner.stopSpinner();
      Notification.push({content: err, error: true, autoHide: true});
    })


  }


}])
;
GrapgController.controller("VertexAsideController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', 'Spinner', 'Aside', 'Icon', '$rootScope', function ($scope, $routeParams, $location, Database, CommandApi, Spinner, Aside, Icon, $rootScope) {


  $scope.database = $routeParams.database;


  Icon.icons().then(function (data) {

    $scope.icons = data;


    $scope.headers = Database.getPropertyFromDoc($scope.doc);
    $scope.headers.unshift("@class");
    $scope.headers.unshift("@rid");
    $scope.active = 'properties';
    if ($scope.doc['@class']) {
      $scope.config = $scope.graph.getClazzConfig($scope.doc['@class']);
    }

    $scope.setDirty = function () {
      $rootScope.$broadcast('graphConfig:changed', true);

    }
    $scope.$watch('config.display', function (val) {
      if (val) {
        $scope.graph.changeClazzConfig($scope.doc['@class'], 'display', val);
      }
    })

    $scope.$watch('config.iconSize', function (val) {

      if (val) {
        $scope.graph.changeClazzConfig($scope.doc['@class'], 'iconSize', val);
      }
    })
    $scope.$watch('config.iconVPadding', function (val) {

      if (val || val === 0) {
        $scope.graph.changeClazzConfig($scope.doc['@class'], 'iconVPadding', val);
      }
    })
    $scope.$watch('config.iconCss', function (val, oldVal) {


      if (val) {
        var mapped = val;

        if ($scope.icons) {
          $scope.icons.forEach(function (d) {
            if (d.css == val) {
              mapped = d.code;

            }
          })


          $scope.graph.changeClazzConfig($scope.doc['@class'], 'iconCss', val);
          $scope.graph.changeClazzConfig($scope.doc['@class'], 'icon', eval('\'\\u' + mapped.toString(16) + '\''));
        }
      } else {
        $scope.graph.changeClazzConfig($scope.doc['@class'], 'iconCss', null);
        $scope.graph.changeClazzConfig($scope.doc['@class'], 'icon', null);
        var val = null;
        if ($scope.graph.getClazzConfig($scope.doc['@class'])) {
          if ($scope.graph.getClazzConfig($scope.doc['@class'])['display']) {
            val = $scope.graph.getClazzConfig($scope.doc['@class'])['display'];
          }
        }
        if (!val) {
          val = "@rid";
        }
        $scope.graph.changeClazzConfig($scope.doc['@class'], 'display', val);
      }
    })
    $scope.$watch('config.fill', function (val) {
      if (val) {
        $scope.graph.changeClazzConfig($scope.doc['@class'], 'fill', val);
      }
    })
    $scope.$watch('config.stroke', function (val) {
      if (val) {
        $scope.graph.changeClazzConfig($scope.doc['@class'], 'stroke', val);
      }
    })
    $scope.$watch('config.r', function (val) {
      if (val) {
        $scope.graph.changeClazzConfig($scope.doc['@class'], 'r', val);
      }
    })

    $scope.$watch('config.displayColor', function (val) {
      if (val) {
        $scope.graph.changeClazzConfig($scope.doc['@class'], 'displayColor', val);
      }
    })
    $scope.$watch('config.displayBackground', function (val) {
      if (val) {
        $scope.graph.changeClazzConfig($scope.doc['@class'], 'displayBackground', val);
      }
    })

    $scope.save = function () {
      $rootScope.$broadcast("graphConfig:onSave");
    }
  });


}]);
GrapgController.controller("EdgeAsideController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', 'Spinner', 'Aside','$rootScope', function ($scope, $routeParams, $location, Database, CommandApi, Spinner, Aside,$rootScope) {


  $scope.database = $routeParams.database;

  $scope.active = 'properties';
  if ($scope.doc) {

    $scope.headers = Database.getPropertyFromDoc($scope.doc);
    $scope.headers.unshift("@class");
    $scope.headers.unshift("@rid");
    $scope.active = 'properties';
    if ($scope.doc['@class']) {
      $scope.config = $scope.graph.getClazzConfig($scope.doc['@class']);
    }

    $scope.$watch('config.display', function (val) {
      if (val) {
        $scope.graph.changeClazzConfig($scope.doc['@class'], 'display', val);
      }
    })
    $scope.$watch('config.icon', function (val) {
      if (val) {
        $scope.graph.changeClazzConfig($scope.doc['@class'], 'icon', val);
      }
    })
    $scope.$watch('config.fill', function (val) {
      if (val) {
        $scope.graph.changeClazzConfig($scope.doc['@class'], 'fill', val);
      }
    })
    $scope.$watch('config.stroke', function (val) {
      if (val) {
        $scope.graph.changeClazzConfig($scope.doc['@class'], 'stroke', val);
      }
    })
    $scope.$watch('config.strokeWidth', function (val) {
      if (val) {
        $scope.graph.changeClazzConfig($scope.doc['@class'], 'strokeWidth', val);
      }
    })

    $scope.save = function () {
      $rootScope.$broadcast("graphConfig:onSave");
    }
  }
}]);
