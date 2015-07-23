angular.module('webappApp').controller('TopicCtrl', function ($scope, $location, $routeParams, Organization, User) {


  $scope.page = 1;
  $scope.query = ""
  $scope.queryText = "";
  if ($routeParams.q) {
    $scope.queryText = $routeParams.q;
    var match = $scope.queryText.match(/(?:[^\s"]+|"[^"]*")+/g);

    match.forEach(function (m) {
      var splitted = m.split(":")
      if (splitted[0] == "text") {
        $scope.query = splitted[1].replace(/"/g, "")
      }
    });

  }
  if ($routeParams.page) {
    $scope.page = $routeParams.page;
  }

  User.whoami().then(function (data) {
    $scope.isMember = User.isMember(ORGANIZATION);
    $scope.isSupport = User.isSupport(ORGANIZATION);
  })
  $scope.searchQuestions = function () {
    Organization.all('topics').customGET("", {q: $scope.queryText, page: $scope.page}).then(function (data) {
      $scope.topics = data.content;
      $scope.pager = data.page;
      $scope.pager.pages = $scope.calculatePages($scope.pager);
    });
  }


  $scope.calculatePages = function (pager) {

    var maxBlocks, maxPage, maxPivotPages, minPage, numPages, pages;
    maxBlocks = 11;
    pages = [];
    var currentPage = pager.number;
    numPages = pager.totalPages;
    if (numPages > 1) {
      pages.push(1);
      maxPivotPages = Math.round((maxBlocks - 5) / 2);
      minPage = Math.max(2, currentPage - maxPivotPages);
      maxPage = Math.min(numPages - 1, currentPage + maxPivotPages * 2 - (currentPage - minPage));
      minPage = Math.max(2, minPage - (maxPivotPages * 2 - (maxPage - minPage)));
      var i = minPage;
      while (i <= maxPage) {
        if ((i === minPage && i !== 2) || (i === maxPage && i !== numPages - 1)) {
          pages.push(null);
        } else {
          pages.push(i);
        }
        i++;
      }
      pages.push(numPages);
      return pages
    }
  }

  $scope.changePage = function (val) {
    if (val > 0 && val <= $scope.pager.totalPages) {
      $scope.page = val;
      $scope.search(true);
    }
  }
  $scope.search = function (page) {
    if (!page) {
      $scope.page = 1;
    }
    var text = "text:\"" + $scope.query + "\"";

    if (!$scope.query || $scope.query == "") {
      text = ""
    }
    $location.search({'q': text, 'page': $scope.page});
  }
  $scope.clear = function () {
    $scope.query = "";
    $scope.search();
  }
  $scope.searchQuestions();
})

angular.module('webappApp').controller('TopicNewCtrl', function ($scope, $location, Organization, User) {

  $scope.preview = false;
  $scope.carriage = true;
  $scope.topic = {}
  User.whoami().then(function (data) {
    $scope.user = data;
    $scope.isMember = User.isMember(ORGANIZATION);
    $scope.isClient = User.isClient(ORGANIZATION);
    $scope.client = User.getClient(ORGANIZATION);
  });

  Organization.all("tags").getList().then(function (data) {
    $scope.tags = data.plain();
  })
  $scope.save = function () {
    Organization.all("topics").post($scope.topic).then(function (data) {
      $location.path("/topics/" + data.number);
    })
  }
});

angular.module('webappApp').controller('TopicEditCtrl', function ($scope, $location, $routeParams, Organization, User) {


  $scope.carriage = true;
  $scope.number = $routeParams.id;

  $scope.newComment = {};
  User.whoami().then(function (data) {
    $scope.isMember = User.isMember(ORGANIZATION);
    $scope.currentUser = data;

    if ($scope.isMember) {
      Organization.all("clients").getList().then(function (data) {
        $scope.clients = data.plain();
      })
    }
  });

  Organization.all("tags").getList().then(function (data) {
    $scope.tags = data.plain();
  })
  Organization.all("topics").one($scope.number).get().then(function (data) {
    $scope.topic = data.plain();
  })

  Organization.all("topics").one($scope.number).all("comments").getList().then(function (data) {
    $scope.comments = data.plain();
  })

  $scope.comment = function () {
    Organization.all("topics").one($scope.number).all("comments").post($scope.newComment).then(function (data) {
      $scope.comments.push(data);
      $scope.newComment = {}
    })
  }
  $scope.changeTitle = function (title) {
    Organization.all("topics").one($scope.number).patch({title: title}).then(function (data) {
      $scope.topic.title = title;
      $scope.newTitle = null;
      $scope.editingTitle = false;
    });
  }

  $scope.$on("tag:added", function (e, tag) {


    Organization.all("topics").one($scope.number).all("tags").post([tag]).then(function (data) {
      $scope.topic.tags.push(tag);
    })

  })
  $scope.$on("tag:removed", function (e, tag) {
    Organization.all("topics").one($scope.number).all("tags").one(tag.uuid).remove().then(function (data) {
      var idx = $scope.topic.tags.indexOf(tag);
      $scope.topic.tags.splice(idx, 1);
    })
  })
});

angular.module('webappApp').controller('TopicBodyController', function ($scope, Repo, Organization) {
  $scope.preview = true;


  $scope.carriage = true;

  $scope.clonedComment = {};
  $scope.cancelEditing = function () {
    $scope.preview = true;
    $scope.topic.body = $scope.clonedComment;
  }
  $scope.edit = function () {
    $scope.preview = false;
    $scope.clonedComment = angular.copy($scope.topic.body);
  }
  $scope.patchComment = function () {
    Organization.all("topics").one($scope.number).patch({body: $scope.topic.body}).then(function (data) {
      $scope.preview = true;
    }).catch(function () {
      $scope.cancelEditing()
    });
  }
})


angular.module('webappApp').controller('TopicCommentController', function ($scope, Organization) {
  $scope.preview = true;

  $scope.carriage = true;

  $scope.clonedComment = {};
  $scope.cancelEditing = function () {
    $scope.preview = true;
    $scope.comment = $scope.clonedComment;
  }
  $scope.edit = function () {
    $scope.preview = false;
    $scope.clonedComment = angular.copy($scope.comment);
  }
  $scope.deleteComment = function () {

    Organization.all("topics").one($scope.number).all("comments").one($scope.comment.uuid).remove().then(function (data) {
      var idx = $scope.comments.indexOf($scope.comment);
      if (idx > -1) {
        $scope.comments.splice(idx, 1);
      }
    }).catch(function () {

    });
  }
  $scope.patchComment = function () {
    Organization.all("topics").one($scope.number).all("comments").one($scope.comment.uuid).patch($scope.comment).then(function (data) {
      $scope.preview = true;
    }).catch(function () {
      $scope.cancelEditing()
    });
  }
})
