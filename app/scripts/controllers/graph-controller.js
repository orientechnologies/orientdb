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
        $scope.doc = DocumentApi.get({ database: $scope.db, document: $scope.rid}, function () {
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
    $scope.label = 'Vertex';
    $scope.fixed = Database.header;
    $scope.canSave = true;
    $scope.canDelete = true;
    $scope.canCreate = true;
    $scope.canAdd = true;
    $scope.popover = {
        title: 'Add edge'
    }

    // Toggle modal
    $scope.showModal = function (rid) {
        var modalScope = $scope.$new(true);
        modalScope.db = $scope.database;
        modalScope.rid = rid;
        var modalPromise = $modal({template: 'views/database/modalEdit.html', persist: false, show: true, backdrop: 'static', scope: modalScope, modalClass: 'editEdge'});

    };
    $scope.showModalConnection = function (label) {
        var modalScope = $scope.$new(true);
        modalScope.db = $scope.database;
        modalScope.originRid = $scope.rid;
        modalScope.container = $scope;
        modalScope.label = label
        var modalPromise = $modal({template: 'views/vertex/modalConnection.html', persist: false, show: true, backdrop: 'static', scope: modalScope, modalClass: 'createEdge'});

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

    }

    $scope.delete = function () {
        var recordID = $scope.doc['@rid']
        Utilities.confirm($scope, $modal, $q, {
            title: 'Warning!',
            body: 'You are removing Vertex ' + recordID + '. Are you sure?',
            success: function () {
                var command = "DELETE Vertex " + recordID;
                CommandApi.queryText({database: $scope.database, language: 'sql', text: command}, function (data) {
                    var clazz = $scope.doc['@class'];
                    $location.path('/database/' + $scope.database + '/browse/' + 'select * from ' + clazz);
                });
            }
        });
    }

    $scope.filterArray = function (arr) {
        if (arr instanceof Array) {
            return arr;
        } else {
            var newArr = new Array;
            newArr.push(arr);
            return newArr;
        }

    }
    $scope.follow = function (rid) {
        var edgeDoc = DocumentApi.get({ database: $scope.database, document: rid}, function () {
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
        var edgeDoc = DocumentApi.get({ database: $scope.database, document: rid}, function () {
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
                var edgeDoc = DocumentApi.get({ database: $scope.database, document: edge}, function () {
                    var command = ""
                    if (Database.isEdge(edgeDoc['@class'])) {
                        command = "DELETE EDGE " + edge;
                    }
                    else {
                        if (group.contains('in_')) {
                            command = "DELETE EDGE FROM " + edge + " TO " + $scope.rid;
                        } else {
                            command = "DELETE EDGE FROM " + $scope.rid + " TO " + edge;
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

GrapgController.controller("VertexModalBrowseController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', function ($scope, $routeParams, $location, Database, CommandApi) {

    $scope.database = Database;
    $scope.limit = 20;
    $scope.queries = new Array;
    $scope.added = new Array;
    $scope.editorOptions = {
        lineWrapping: true,
        lineNumbers: true,
        readOnly: false,
        theme: 'ambiance',
        mode: 'text/x-sql',
        metadata: Database,
        extraKeys: {
            "Ctrl-Enter": function (instance) {
                $scope.$apply(function () {
                    $scope.query();
                });
            },
            "Ctrl-Space": "autocomplete"
        }
    };
    $scope.query = function () {
        CommandApi.queryText({database: $routeParams.database, language: 'sql', text: $scope.queryText, limit: $scope.limit, verbose: false }, function (data) {
            if (data.result) {
                $scope.headers = Database.getPropertyTableFromResults(data.result);
                $scope.results = data.result;
            }
            if ($scope.queries.indexOf($scope.queryText) == -1)
                $scope.queries.push($scope.queryText);
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

GrapgController.controller("GraphController", ['$scope', '$routeParams', '$location', '$modal', 'Database', 'CommandApi', 'Spinner', 'Aside', 'DocumentApi', 'localStorageService', function ($scope, $routeParams, $location, $modal, Database, CommandApi, Spinner, Aside, DocumentApi, localStorageService) {


    var data = [];

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
            "Ctrl-Space": "autocomplete"

        },
        onLoad: function (_cm) {
            $scope.cm = _cm;
            if ($routeParams.query) {
                $scope.queryText = $routeParams.query;


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

        }
    };

    var config = localStorageService.get("graphConfig");
    if (!config) {
        config = {
            height: 500,
            width: 1200,
            classes: {

            },
            node: {
                r: 30
            }

        }
    }
    $scope.graphOptions = {
        data: data,
        onLoad: function (graph) {
            $scope.graph = graph;

            $scope.graph.on('node/click', function (v) {

//                $scope.doc = v.source;
//
//                Aside.show({scope: $scope, template: 'views/database/graph/asideVertex.html', show: true});
            });
            $scope.graph.on('node/dblclick', function (v) {

                var q = "select expand(unionAll(bothE(),both()) )  from " + v['@rid'];
                CommandApi.queryText({database: $routeParams.database, contentType: 'JSON', language: $scope.language, text: q, limit: -1, shallow: false, verbose: false}, function (data) {

                    $scope.graph.data(data.result).redraw();
                })
            });

            $scope.graph.on('node/load', function (v, callback) {
                DocumentApi.get({ database: $routeParams.database, document: v.source}, function (doc) {
                    callback(doc);
                });
            });
        },
        metadata: Database.getMetadata(),
        config: config,
        actions: [
            {
                name: "Edit",
                onClick: function (v) {
                    $scope.showModal(v.source["@rid"]);
                }
            },
            {
                name: "Out",
                onClick: function (v) {

                },
                actions: function (v) {

                    var acts = [];
                    var outgoings = Database.getEdge(v.source, 'out_');
                    outgoings.forEach(function (elem) {
                        acts.push(
                            {
                                name: elem.replace("out_", ""),
                                onClick: function (label) {
                                    console.log(label);
                                }
                            }
                        )
                    })
                    return acts;
                }

            },
            {
                name: "In",
                onClick: function (v) {

                },
                actions: function (v) {

                    var acts = [];
                    var outgoings = Database.getEdge(v.source, 'in_');
                    outgoings.forEach(function (elem) {
                        acts.push(
                            {
                                name: elem.replace("in_", ""),
                                onClick: function (label) {
                                    console.log(label);
                                }
                            }
                        )
                    })
                    return acts;
                }
            },
            {
                name: "View",
                onClick: function (v) {
                    $scope.doc = v.source;
                    Aside.show({scope: $scope, template: 'views/database/graph/asideVertex.html', show: true});
                }
            }
        ]


    }
    $scope.showModal = function (rid) {
        var modalScope = $scope.$new(true);
        modalScope.db = $routeParams.database;
        modalScope.database = $routeParams.database;
        modalScope.rid = rid;
        $modal({template: 'views/database/modalEdit.html', persist: false, show: true, backdrop: 'static', scope: modalScope, modalClass: 'editEdge'});

    };
    $scope.saveConfig = function () {
        localStorageService.add("graphConfig", $scope.graph.getConfig());
    }
    $scope.query = function () {

        Spinner.start();
        if ($scope.queryText.startsWith('g.')) {
            $scope.language = 'gremlin';
        } else {
            $scope.language = 'sql';
        }
        CommandApi.queryText({database: $routeParams.database, contentType: 'JSON', language: $scope.language, text: $scope.queryText, limit: 20, shallow: false, verbose: false}, function (data) {

            if (data.result) {

                $scope.graph.data(data.result).redraw();
            }
            Spinner.stopSpinner();
        }, function (data) {
            Spinner.stopSpinner();
        });

    }

    if ($routeParams.q) {
        $scope.queryText = $routeParams.q;
        $scope.query();
    }
}])
;
GrapgController.controller("VertexAsideController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', 'Spinner', 'Aside', function ($scope, $routeParams, $location, Database, CommandApi, Spinner, Aside) {

    $scope.database = $routeParams.database;
    $scope.headers = Database.getPropertyFromDoc($scope.doc);
    $scope.active = 'properties';
    if ($scope.doc['@class']) {
        $scope.config = $scope.graph.getClazzConfig($scope.doc['@class']);
    }

    $scope.$watch('config.display', function (val) {
        if (val) {
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
}]);