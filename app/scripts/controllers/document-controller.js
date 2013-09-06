var DocController = angular.module('document.controller', []);
DocController.controller("DocumentEditController", ['$scope', '$injector', '$routeParams', '$location', '$modal', '$dialog', '$q', 'DocumentApi', 'Database', 'Notification', function ($scope, $injector, $routeParams, $location, $modal, $dialog, $q, DocumentApi, Database, Notification) {

    $injector.invoke(BaseEditController, this, {$scope: $scope});
    $scope.fixed = Database.header;
    $scope.canSave = true;
    $scope.canDelete = true;
    $scope.canCreate = true;
    $scope.canAdd = true;

    // Toggle modal
    $scope.showModal = function (rid) {
        modalScope = $scope.$new(true);
        modalScope.db = $scope.database;
        modalScope.rid = rid;
        var modalPromise = $modal({template: 'views/document/modalEdit.html', persist: true, show: false, backdrop: 'static', modalClass: 'editEdge', scope: modalScope});
        $q.when(modalPromise).then(function (modalEl) {
            modalEl.modal('show');
        });
    };

    if (!$scope.doc) {
        $scope.reload();
    } else {
        $scope.headers = Database.getPropertyFromDoc($scope.doc);
        $scope.isGraph = Database.isGraph($scope.doc['@class']);
        $scope.incomings = Database.getEdge($scope.doc, 'in');
        $scope.outgoings = Database.getEdge($scope.doc, 'out');
        $scope.outgoings = $scope.outgoings.concat((Database.getLink($scope.doc)));
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
    $scope.deleteLink = function (group, rid) {
        var index = $scope.doc[group].indexOf(rid);
        $scope.doc[group].splice(index, 1);
    }

    $scope.showModalConnection = function (label) {
        var modalScope = $scope.$new(true);
        modalScope.type = Database.listPropertyForClass($scope.doc['@class'], label);
        modalScope.db = database;
        modalScope.originRid = $scope.rid;
        modalScope.container = $scope;
        modalScope.label = label
        var modalPromise = $modal({template: 'views/document/modalConnection.html', persist: true, show: false, backdrop: 'static', scope: modalScope, modalClass: 'createEdge'});
        $q.when(modalPromise).then(function (modalEl) {
            modalEl.modal('show');
        });
    }
}]);
DocController.controller("DocumentCreateController", ['$scope', '$routeParams', '$location', 'DocumentApi', 'Database', 'Notification', function ($scope, $routeParams, $location, DocumentApi, Database, Notification) {


    var database = $routeParams.database;
    var clazz = $routeParams.clazz
    $scope.fixed = Database.header;
    $scope.doc = DocumentApi.createNewDoc(clazz);
    $scope.headers = Database.getPropertyFromDoc($scope.doc);
    $scope.save = function () {
        DocumentApi.createDocument(database, $scope.doc['@rid'], $scope.doc, function (data) {
            Notification.push({content: JSON.stringify(data)});
            $location.path('#/database/' + database + '/browse/edit/' + data['@rid'].replace('#', ''));
        });

    }
}]);
DocController.controller("DocumentModalController", ['$scope', '$routeParams', '$location', 'DocumentApi', 'Database', 'Notification', function ($scope, $routeParams, $location, DocumentApi, Database, Notification) {

    $scope.types = Database.getSupportedTypes();
    $scope.reload = function () {
        $scope.doc = DocumentApi.get({ database: $scope.db, document: $scope.rid}, function () {
            $scope.headers = Database.getPropertyFromDoc($scope.doc);
        }, function (error) {
            Notification.push({content: JSON.stringify(error)});
            $location.path('/404');
        });
    }
    $scope.save = function () {
        DocumentApi.updateDocument($scope.db, $scope.rid, $scope.doc, function (data) {
            Notification.push({content: data});
        });

    }
    $scope.addField = function (name, type) {
        if (name) {
            $scope.doc[name] = null;
            var types = $scope.doc['@fieldTypes'];
            if (types) {
                types = types + ',' + name + '=' + Database.getMappingFor(type);
            } else {
                types = name + '=' + Database.getMappingFor(type);
            }
            $scope.doc['@fieldTypes'] = types;
            $scope.headers.push(name);
        }
    }
    $scope.deleteField = function (name) {
        delete $scope.doc[name];
        var idx = $scope.headers.indexOf(name);
        $scope.headers.splice(idx, 1);
    }
    $scope.reload();
}]);
DocController.controller("EditController", ['$scope', '$routeParams', '$location', 'DocumentApi', 'Database', 'Notification', function ($scope, $routeParams, $location, DocumentApi, Database, Notification) {

    var database = $routeParams.database;
    var rid = $routeParams.rid;
    $scope.doc = DocumentApi.get({ database: database, document: rid}, function () {

        $scope.template = Database.isGraph($scope.doc['@class']) ? 'views/database/editVertex.html' : 'views/database/editDocument.html'
    }, function (error) {
        Notification.push({content: JSON.stringify(error)});
        $location.path('404');
    });


}]);
DocController.controller("CreateController", ['$scope', '$routeParams', '$location', 'DocumentApi', 'Database', 'Notification', function ($scope, $routeParams, $location, DocumentApi, Database, Notification) {

    var database = $routeParams.database;
    var clazz = $routeParams.clazz
    $scope.fixed = Database.header;
    $scope.doc = DocumentApi.createNewDoc(clazz);
    $scope.headers = Database.getPropertyFromDoc($scope.doc);
    $scope.isNew = true;
    $scope.template = Database.isGraph(clazz) ? 'views/database/editVertex.html' : 'views/database/editDocument.html'

}]);
DocController.controller("DocumentModalBrowseController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', function ($scope, $routeParams, $location, Database, CommandApi) {

    $scope.database = Database;
    $scope.limit = 20;
    $scope.queries = new Array;
    $scope.added = new Array;
    $scope.queryText = "select * from " + $scope.type.linkedClass;
    $scope.editorOptions = {
        lineWrapping: true,
        lineNumbers: true,
        readOnly: false,
        theme: 'ambiance',
        mode: 'text/x-sql',
        extraKeys: {
            "Ctrl-Enter": function (instance) {
                $scope.$apply(function(){
                    $scope.query();
                });
            }
        }
    };
    $scope.query = function () {
        CommandApi.queryText({database: $routeParams.database, language: 'sql', text: $scope.queryText, limit: $scope.limit}, function (data) {
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
    $scope.createLink = function () {
        if (!$scope.container.doc[$scope.label]) {
            $scope.container.doc[$scope.label] = new Array;
        }
        $scope.container.doc[$scope.label] = $scope.container.doc[$scope.label].concat($scope.added);
        $scope.container.save();

    }
}]);


function BaseEditController($scope, $routeParams,$route, $location, $modal, $dialog, $q, DocumentApi, Database, Notification, CommandApi) {
    $scope.database = $routeParams.database;
    $scope.rid = $routeParams.rid;
    $scope.label = 'Document';


    $scope.save = function () {
        if (!$scope.isNew) {
            DocumentApi.updateDocument($scope.database, $scope.rid, $scope.doc, function (data) {
                Notification.push({content: data});
                $route.reload();
            });
        } else {
            DocumentApi.createDocument($scope.database, $scope.doc['@rid'], $scope.doc, function (data) {
                Notification.push({content: JSON.stringify(data)});
                $location.path('database/' + $scope.database + '/browse/edit/' + data['@rid'].replace('#', ''));
            });
        }
    }


    $scope.reload = function () {

        $scope.doc = DocumentApi.get({ database: $scope.database, document: $scope.rid}, function () {
            $scope.headers = Database.getPropertyFromDoc($scope.doc);
            $scope.isGraph = Database.isGraph($scope.doc['@class']);
            $scope.incomings = Database.getEdge($scope.doc, 'in_');
            $scope.outgoings = Database.getEdge($scope.doc, 'out_');
            $scope.outgoings = $scope.outgoings.concat((Database.getLink($scope.doc)));
        }, function (error) {
            Notification.push({content: JSON.stringify(error)});
            $location.path('404');
        });
    }

    $scope.getLabelFor = function (label) {
        var props = Database.listPropertyForClass($scope.doc['@class'], label);
        return label + (props.linkedClass != undefined ? " (" + (props.linkedClass) + ")" : "" );
    }
    $scope.delete = function () {

        var recordID = $scope.doc['@rid'];
        var clazz = $scope.doc['@class'];
        Utilities.confirm($scope, $dialog, {
            title: 'Warning!',
            body: 'You are removing ' + $scope.label + ' ' + recordID + '. Are you sure?',
            success: function () {
                var command = "DELETE Vertex " + recordID;
                CommandApi.queryText({database: $scope.database, language: 'sql', text: command}, function (data) {
                    var clazz = $scope.doc['@class'];
                    $location.path('database/' + $scope.database + '/browse/' + 'select * from ' + clazz);
                });
            }
        });
    }

    $scope.deleteField = function (name) {
        Utilities.confirm($scope, $dialog, {
            title: 'Warning!',
            body: 'You are removing field ' + name + ' from ' + $scope.label + ' ' + $scope.doc['@rid'] + '. Are you sure?',
            success: function () {
                delete $scope.doc[name];
                var idx = $scope.headers.indexOf(name);
                $scope.headers.splice(idx, 1);
            }
        });
    }

    $scope.addField = function (name, type) {
        if (name) {
            $scope.doc[name] = null;
            var types = $scope.doc['@fieldTypes'];
            if (type == 'BOOLEAN') {
                $scope.doc[name] = false;
            }
            if (type == 'INTEGER') {
                $scope.doc[name] = 0;
            }
            if (Database.getMappingFor(type)) {
                if (types) {
                    types = types + ',' + name + '=' + Database.getMappingFor(type);
                } else {
                    types = name + '=' + Database.getMappingFor(type);
                }
                $scope.doc['@fieldTypes'] = types;
            }
            $scope.headers.push(name);
        } else {
            var modalScope = $scope.$new(true);
            modalScope.addField = $scope.addField;
            modalScope.types = Database.getSupportedTypes();
            var modalPromise = $modal({template: 'views/database/newField.html', persist: true, show: false, backdrop: 'static', scope: modalScope});
            $q.when(modalPromise).then(function (modalEl) {
                modalEl.modal('show');
            });
        }

    }
    $scope.follow = function (rid) {
        $scope.navigate(rid);
    }
    $scope.navigate = function (rid) {
        $location.path('database/' + $scope.database + '/browse/edit/' + rid.replace('#', ''));
    }
    $scope.create = function () {
        $location.path('database/' + $scope.database + '/browse/create/' + $scope.doc['@class']);
    }
}