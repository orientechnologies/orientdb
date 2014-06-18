var dbModule = angular.module('database.controller', ['database.services', 'bookmarks.services']);
dbModule.controller("BrowseController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', 'localStorageService', 'Spinner', '$modal', '$q', '$window', 'Bookmarks', 'Notification', function ($scope, $routeParams, $location, Database, CommandApi, localStorageService, Spinner, $modal, $q, $window, Bookmarks, Notification) {

    $scope.database = Database;
    $scope.limit = 20;

    $scope.bookmarksClass = "";
    $scope.items = [
        {name: "history", title: "History"},
        {name: "bookmarks", title: "Bookmarks"}
    ];

    $scope.item = {};
    $scope.queries = [];
    $scope.$watch("queryText", function (val) {
        $scope.item.query = val;
    });
    $scope.context = $scope.items[0];
    $scope.nContext = $scope.items[1];
    $scope.nextPage = function () {
        var idx = $scope.items.indexOf($scope.context);
        var newIdx = (idx < $scope.items.length - 1) ? idx + 1 : 0;
        $scope.context = $scope.items[newIdx];
        var nextIdx = (newIdx < $scope.items.length - 1) ? newIdx + 1 : 0;
        $scope.nContext = $scope.items[nextIdx];
    }
    $scope.countPage = 5;

    $scope.setBookClass = function () {
        if ($scope.bookmarksClass == "") {
            $scope.bookmarksClass = "show";
        } else {
            $scope.bookmarksClass = "";
        }
    }

    if (Database.hasClass(Bookmarks.CLAZZ)) {
        Bookmarks.getAll(Database.getName());
    } else {
        Bookmarks.init(Database.getName()).then(function () {
            Bookmarks.getAll(Database.getName());
        });
    }

    $scope.hideSettings = localStorageService.get("hideSettings");
    if ($scope.hideSettings == null) {
        $scope.hideSettings = false;
        localStorageService.add("hideSettings", $scope.hideSettings);
    } else {
        $scope.hideSettings = JSON.parse($scope.hideSettings);
    }
    $scope.keepLimit = localStorageService.get("keepLimit");
    if (!$scope.keepLimit) {
        $scope.keepLimit = 10;
        localStorageService.add("keepLimit", $scope.keepLimit);
    }
    $scope.shallow = localStorageService.get("shallowCollection");

    if ($scope.shallow == null) {
        $scope.shallow = true;
        localStorageService.add("shallowCollection", $scope.shallow);

    } else {
        $scope.shallow = JSON.parse($scope.shallow);
    }
    $scope.countPageOptions = [5, 10, 20, 50, 100, 500, 1000, 2000, 5000];
    var dbTime = localStorageService.get("Timeline");
    if (!dbTime || dbTime instanceof  Array) {
        dbTime = new Object;
        localStorageService.add("Timeline", dbTime);
    }

    $scope.timeline = dbTime[Database.getName()];
    if (!$scope.timeline) {
        $scope.timeline = new Array;
        var localTime = localStorageService.get("Timeline");
        localTime[Database.getName()] = $scope.timeline;
        localStorageService.add("Timeline", localTime);


    }
    $scope.tm = $scope.timeline;
    Database.setWiki("https://github.com/orientechnologies/orientdb-studio/wiki/Query");

    $scope.language = 'sql';

    $scope.table = true;
    $scope.contentType = ['JSON', 'CSV'];
    $scope.selectedContentType = $scope.contentType[0];
    $scope.editorOptions = {
        lineWrapping: true,
        lineNumbers: true,
        readOnly: false,
//        theme: 'ambiance',
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
                //$scope.cm.setValue($scope.queryText);

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

    $scope.query = function () {

        var queryBuffer = "" + $scope.queryText;
        var selection = $scope.cm.getSelection();
        if (selection && selection != "") {
            console.log(selection);
            queryBuffer = "" + selection;
        }

        queryBuffer = queryBuffer.trim();
        queryBuffer = queryBuffer.replace(/\n/g, " ");
        Spinner.start(function () {
            CommandApi.interrupt(Database.getName(), queryBuffer).then(function () {
                Spinner.stop();
            });
        });


        if (queryBuffer.startsWith('g.')) {
            $scope.language = 'gremlin';
        }
        if (queryBuffer.startsWith('#')) {
            $location.path('/database/' + $routeParams.database + '/browse/edit/' + queryBuffer.replace('#', ''));
        }

        var conttype;
        if ($scope.selectedContentType == 'CSV')
            conttype = 'text/csv';

        CommandApi.queryText({database: $routeParams.database, contentType: conttype, language: $scope.language, text: queryBuffer, limit: $scope.limit, shallow: $scope.shallow, verbose: false}, function (data) {

            if (data.result) {

                var item = new Object;
                item.query = $scope.queryText;
                if (selection && selection != "") {
                    item.query = selection;
                }

                item.language = $scope.language;
                $scope.headers = Database.getPropertyTableFromResults(data.result);
                if ($scope.headers.length == 00) {
                    $scope.alerts = new Array;
                    $scope.alerts.push({content: "No records found."});
                }
                $scope.rawData = JSON.stringify(data);
                $scope.resultTotal = data.result;
                $scope.results = data.result.slice(0, $scope.countPage);
                $scope.currentPage = 1;
                $scope.numberOfPage = new Array(Math.ceil(data.result.length / $scope.countPage));
                item.headers = $scope.headers;
                item.rawData = $scope.rawData;
                item.resultTotal = $scope.resultTotal;
                item.results = $scope.results;
                item.currentPage = $scope.currentPage;
                item.numberOfPage = $scope.numberOfPage;
                item.countPage = 5;
                item.countPageOptions = [5, 10, 20, 50, 100, 500, 1000, 2000, 5000];
                item.notification = data.notification;
                $scope.timeline.unshift(item);
                if ($scope.timeline.length > $scope.keepLimit) {
                    $scope.timeline.splice($scope.timeline.length - 1, 1);
                }
                var dbTime = localStorageService.get("Timeline");
                dbTime[Database.getName()] = $scope.timeline;
                localStorageService.add("Timeline", dbTime);

                Spinner.stopSpinner();
                $scope.context = $scope.items[0];
                $scope.nContext = $scope.items[1];
                Notification.clear();
            }
            Spinner.stopSpinner();
        }, function (data) {
            Spinner.stopSpinner();
            $scope.headers = undefined;
            $scope.resultTotal = undefined;
            $scope.results = undefined;
        });

    }

    $scope.clear = function () {

        Utilities.confirm($scope, $modal, $q, {
            title: 'Warning!',
            body: 'You are clearing history. Are you sure?',
            success: function () {
                $scope.timeline = new Array;
                var dbTime = localStorageService.get("Timeline");
                dbTime[Database.getName()] = $scope.timeline;
                localStorageService.add("Timeline", dbTime);
            }
        });

    }

    $scope.removeItem = function (item) {
        var idx = $scope.timeline.indexOf(item);
        $scope.timeline.splice(idx, 1);
        localStorageService.add("Timeline", $scope.timeline);
    }


    $scope.$watch("limit", function (data) {
        $scope.limit = data;
    });
    $scope.$watch("selectedContentType", function (data) {
        $scope.selectedContentType = data;
    });
    $scope.$watch("shallow", function (data) {
        $scope.shallow = data;
        localStorageService.add("shallowCollection", data);
    });
    $scope.$watch("keepLimit", function (data) {
        $scope.keepLimit = data;
        localStorageService.add("keepLimit", data);
    });

//    $scope.loadMore = function () {
//        var len = $scope.queries.length;
//        var lenTime = $scope.timeline.length;
//        if (len < lenTime)
//            $scope.queries.push($scope.timeline[len]);
//    };
//    $scope.loadMore();
//    $scope.loadMore();

}]);
dbModule.controller("QueryController", ['$scope', '$routeParams', '$filter', '$location', 'Database', 'CommandApi', 'localStorageService', 'Spinner', 'ngTableParams', 'scroller', '$ojson', function ($scope, $routeParams, $filter, $location, Database, CommandApi, localStorageService, Spinner, ngTableParams, scroller, $ojson) {


    var data = $scope.item.resultTotal;

    if ($scope.item.rawData instanceof Object) {
        $scope.item.rawData = JSON.stringify($scope.item.rawData);
    }
    $scope.bookIcon = 'icon-star';
    $scope.viewerOptions = {
        lineWrapping: true,
        lineNumbers: true,
        readOnly: true,
        mode: 'javascript',
        onLoad: function (_cm) {
            $scope.vcm = _cm;
            //$scope.vcm.setValue($ojson.format($scope.item.rawData));
        }

    };
    $scope.changeIcon = function () {
        $scope.bookIcon = 'icon-star';
    }
    $scope.tableParams = new ngTableParams({
        page: 1,            // show first page
        count: 10          // count per page

    }, {
        total: data.length, // length of data
        getData: function ($defer, params) {
            // use build-in angular filter
            //            var orderedData = params.sorting() ?
            //                $filter('orderBy')(data, params.orderBy()) :
            //                data;
            $defer.resolve(data.slice((params.page() - 1) * params.count(), params.page() * params.count()));
        }
    });

    $scope.tableParams.settings().counts = [10, 25, 50, 100, 1000, 5000];
    $scope.switchPage = function (index) {
        if (index != $scope.item.currentPage) {
            $scope.item.currentPage = index;
            $scope.item.results = $scope.item.resultTotal.slice(
                (index - 1) * $scope.item.countPage,
                index * $scope.item.countPage
            );
        }
    }
    $scope.previous = function () {
        if ($scope.item.currentPage > 1) {
            $scope.switchPage($scope.item.currentPage - 1);
        }
    }
    $scope.next = function () {

        if ($scope.item.currentPage < $scope.item.numberOfPage.length) {
            $scope.switchPage($scope.item.currentPage + 1);
        }
    }
    $scope.$watch("item.countPage", function (data) {
        if ($scope.item.resultTotal) {
            $scope.item.results = $scope.item.resultTotal.slice(0, $scope.item.countPage);
            $scope.item.currentPage = 1;
            $scope.item.numberOfPage = new Array(Math.ceil($scope.item.resultTotal.length / $scope.item.countPage));
        }
    });
    $scope.openRecord = function (doc) {
        $location.path("/database/" + $scope.database.getName() + "/browse/edit/" + doc["@rid"].replace('#', ''));
    }
    $scope.changeQuery = function () {
        $scope.queryText = $scope.item.query;
        scroller.scrollTo(0, 0, 2000);
        $scope.cm.focus();

        $scope.cm.setValue($scope.queryText);
        $scope.cm.setCursor($scope.cm.lineCount());

    }


}]);
dbModule.controller("QueryConfigController", ['$scope', '$routeParams', 'localStorageService', function ($scope, $routeParams, localStorageService) {


    $scope.$watch("limit", function (data) {
        $scope.$parent.limit = data;
    });
    $scope.$watch("selectedContentType", function (data) {
        $scope.$parent.selectedContentType = data;
    });
    $scope.$watch("shallow", function (data) {
        $scope.$parent.shallow = data;
        localStorageService.add("shallowCollection", data);
    });
    $scope.$watch("keepLimit", function (data) {
        $scope.$parent.keepLimit = data;
        localStorageService.add("keepLimit", data);
    });
    $scope.$watch("hideSettings", function (data) {
        $scope.$parent.hideSettings = data;
        localStorageService.add("hideSettings", data);
        if ($scope.hide) {
            $scope.hide();
        }
    });
    $scope.$parent.$watch("limit", function (data) {
        $scope.limit = data;
    });
    $scope.$parent.$watch("selectedContentType", function (data) {
        $scope.selectedContentType = data;
    });
    $scope.$parent.$watch("shallow", function (data) {
        $scope.shallow = data;
    });
    $scope.$parent.$watch("keepLimit", function (data) {
        $scope.keepLimit = data;
    });
    $scope.$parent.$watch("hideSettings", function (data) {
        $scope.hideSettings = data;
    });


}]);
dbModule.controller("BookmarkNewController", ['$scope', '$rootScope', 'Bookmarks', 'DocumentApi', 'Database', function ($scope, $rootScope, Bookmarks, DocumentApi, Database) {


    $scope.bookmark = DocumentApi.createNewDoc(Bookmarks.CLAZZ);

    $scope.bookmark.name = $scope.item.query;
    $scope.bookmark.query = $scope.item.query;

    Bookmarks.getTags(Database.getName()).then(function (data) {
        $scope.tags = data;
        $scope.select2Options = {
            'multiple': true,
            'simple_tags': true,
            'tags': $scope.tags  // Can be empty list.
        };
        $scope.viewTags = true;
    });

    $scope.viewTags = false;
    $scope.addBookmark = function () {
        Bookmarks.addBookmark(Database.getName(), $scope.bookmark).then(function () {
            $rootScope.$broadcast('bookmarks:changed');
            $scope.hide();
        });
    }
}]);
dbModule.controller("BookmarkEditController", ['$scope', '$rootScope', 'Bookmarks', 'DocumentApi', 'Database', function ($scope, $rootScope, Bookmarks, DocumentApi, Database) {

    $scope.bookmark = $scope.bk;


    Bookmarks.getTags(Database.getName()).then(function (data) {
        $scope.tags = data;
        $scope.select2Options = {
            'multiple': true,
            'simple_tags': true,
            'tags': $scope.tags  // Can be empty list.
        };
        $scope.viewTags = true;
    });

    $scope.viewTags = false;
    $scope.addBookmark = function () {
        Bookmarks.update(Database.getName(), $scope.bookmark).then(function () {
            $rootScope.$broadcast('bookmarks:changed');
            $scope.hide();
        });
    }
}]);
dbModule.controller("BookmarkController", ['$scope', 'Bookmarks', 'DocumentApi', 'Database', 'scroller', function ($scope, Bookmarks, DocumentApi, Database, scroller) {


    $(document).bind("keypress", function (e) {
        if ($scope.$parent.bookmarksClass == "show") {
            $scope.$apply(function () {
                $scope.closeIfReturn(e);
            });
        }
    });
    $scope.closeIfReturn = function (e) {
        if (e.keyCode == '27') {
            $scope.click();
        }
    }
    $scope.$on("bookmarks:changed", function (event) {
        Bookmarks.getAll(Database.getName()).then(function (data) {
            $scope.bks = data.result;

        });
    });

    Bookmarks.getAll(Database.getName()).then(function (data) {
        $scope.bks = data.result
    });

    $scope.click = function () {

        $scope.$parent.setBookClass();

    }
    $scope.hover = function (bk) {
        $scope.selected = bk;
    }
    $scope.run = function (r) {
        $scope.queryText = r.query;
        scroller.scrollTo(0, 0, 2000);
        $scope.cm.focus();

        $scope.cm.setValue($scope.queryText);
        $scope.cm.setCursor($scope.cm.lineCount());
        $scope.$parent.setBookClass();
    }

    $scope.remove = function (r) {
        Bookmarks.remove(Database.getName(), r).then(function (data) {
            var idx = $scope.bks.indexOf(r);
            $scope.bks.splice(idx, 1);
        });

    }
    $scope.update = function (r) {
        Bookmarks.update(Database.getName(), r).then(function (data) {
            var idx = $scope.bks.indexOf(r);
            $scope.bks.splice(idx, 1, data);
        });
    }
}]);



