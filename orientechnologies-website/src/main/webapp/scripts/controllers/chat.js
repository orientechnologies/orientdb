'use strict';

/**
 * @ngdoc function
 * @name webappApp.controller:AboutCtrl
 * @description
 * # AboutCtrl
 * Controller of the webappApp
 */
angular.module('webappApp')
  .controller('ChatCtrl', function ($scope, Organization, $routeParams, $route, User, $timeout, BreadCrumb, $location, ChatService, $rootScope, $filter) {


    $scope.changingRoute = false;
    $scope.isNew = false;
    $scope.connectionLost = false;
    $scope.placeholder = "Click here to type a message(Supports Markdown). Enter to send.";
    $scope.clientId = $routeParams.id;


    $scope.sending = false;

    $scope.log = null;
    $scope.connected = ChatService.connected;


    $scope.closeMe = function () {
      $('#roomsMobile').offcanvas('toggle');
    }

    $scope.$on("$routeChangeStart", function () {
      $scope.changingRoute = true;
      if ($scope.log) {
        $scope.log.remove();

        $scope.log = null;
      }
      $scope.connectionLost = false;
    });
    $scope.$on('connection-closed', function () {

    })
    $scope.$on('connection-lost', function () {
      console.log('connection lost');


      if (!$scope.log) {


        $scope.log = humane.create({baseCls: 'humane-jackedup', timeout: 0, addnCls: 'humane-jackedup-error'})
        $scope.log.log("Connection lost. Reconnecting now.");

        $scope.$apply(function () {
          $scope.connectionLost = true;
        });

      }
    })

    $scope.$on('connection-acquired', function () {
      if ($scope.log) {
        $scope.log.remove()
        $scope.log = null;
        $scope.connectionLost = false;
        var jacked = humane.create({baseCls: 'humane-jackedup', addnCls: 'humane-jackedup-success'})
        jacked.log("Connection restored");
        getMessages();
      }
    })
    $scope.$on('msg-received', function (e, msg) {

      if (msg.sender.name != $scope.currentUser.name) {

        if (msg.edited) {

          if ($scope.clientId == msg.clientId) {
            replaceMsg(msg);
            visit();
          }
        } else {
          if ($scope.clientId == msg.clientId) {
            $scope.$apply(function () {
              addNewMessage(msg);
              visit()

            });
          } else {
            $scope.$apply(function () {
              $scope.clients.forEach(function (c) {
                if (c.clientId == msg.clientId) {
                  c.timestamp = new Date().getTime();
                }
              })
            });
          }
        }
      }
    });


    $scope.loadMore = function () {
      Organization.all("clients").one($scope.clientId).all("room").customGET("", {
        before: $scope.messages[0].messages[0].id
      }).then(function (data) {
        var msg = aggregateMessage(data.reverse());
        msg.reverse();
        msg.forEach(function (e) {
          $scope.messages.unshift(e);
        })
      }).catch(function (e) {

      });
    }


    var findGroup = function (groups, date) {
      return groups.filter(function (g) {
        return g.date == date;
      })
    }
    var visit = function () {
      Organization.all("clients").one($scope.clientId).all("room").all('checkin').patch().then(function (data) {
        $scope.clients.forEach(function (c) {
          if (c.clientId == $scope.clientId) {
            c.lastVisit = new Date().getTime();
          }
        });
      }).catch(function (e) {

      })
    }
    var aggregateMessage = function (msg) {
      var newMsg = []
      var lastTime = null;
      var lastUser = null
      msg.forEach(function (m) {

        if (lastUser == m.sender.name) {
          var momentLast = moment(lastTime);
          var momentCurrent = moment(m.date);
          var diff = momentCurrent.diff(momentLast, "minutes");
          if (diff < 20) {
            var group = findGroup(newMsg, lastTime);
            group[0].messages.push(m);
          } else {
            lastUser = m.sender.name;
            lastTime = m.date;
            newMsg.push({
              sender: m.sender,
              date: lastTime,
              messages: [m]
            })
          }

        } else {
          lastUser = m.sender.name;
          lastTime = m.date;
          newMsg.push({
            sender: m.sender,
            date: lastTime,
            messages: [m]
          })
        }
      });
      return newMsg;
    }

    function getMessages() {
      if ($scope.clientId) {
        Organization.all("clients").one($scope.clientId).all("room").getList().then(function (data) {
          $scope.messages = aggregateMessage(data.plain().reverse())
        }).catch(function (e) {
          if (e.status == 400 && e.data) {
            $scope.isNew = true;
          }
        });
        Organization.all("clients").one($scope.clientId).all("room").all('actors').getList().then(function (data) {
          $scope.actors = data.plain();
        });
        visit()
      }
    }

    var replaceMsg = function (msg) {
      $scope.messages.forEach(function (g) {
        g.messages.forEach(function (m) {
          if (m.id === msg.id) {
            m.body = msg.body;
          }
        });
      })
    }
    var addNewMessage = function (message) {


      var len = $scope.messages.length;
      if (len > 0) {
        var lastTime = $scope.messages[len - 1].date;
        var lastGroup = $scope.messages[len - 1];
        if (lastGroup.sender.name == message.sender.name) {

          var momentLast = moment(new Date(parseInt(lastTime)));
          var momentCurrent = moment(new Date(message.date));
          var diff = momentCurrent.diff(momentLast, "minutes");
          if (diff < 20) {
            lastGroup.messages.push(message);
          } else {
            $scope.messages.push({
              date: message.date,
              sender: message.sender,
              messages: [message]
            })
          }
        } else {
          $scope.messages.push({
            date: message.date,
            sender: message.sender,
            messages: [message]
          })
        }
      } else {
        $scope.messages.push({
          date: message.date,
          sender: message.sender,
          messages: [message]
        })
      }
    }
    User.whoami().then(function (data) {
      $scope.currentUser = data;
      $scope.$watch(function () {
        return ChatService.clients;
      }, function (clients) {
        if (clients.length > 0) {
          $scope.clients = clients;
          if (!$scope.clientId) {

            $scope.clients = $filter('orderBy')($scope.clients, "timestamp", true);
            $scope.clientId = $scope.clients[0].clientId.toString();
            $scope.client = $scope.clients[0];
          } else {

            $scope.clients.forEach(function (c) {
              if (c.clientId == $scope.clientId) {
                $scope.client = c;
              }
            })
          }
          BreadCrumb.title = 'Room ' + $scope.client.name;
          getMessages();
        }
      })


    });


    $scope.createChat = function () {
      Organization.all("clients").one($scope.clientId).all("room").post().then(function (data) {
        var jacked = humane.create({baseCls: 'humane-jackedup', addnCls: 'humane-jackedup-success'})
        jacked.log("Room created");
        $route.reload();
      });
    }
    $scope.sendMessage = function () {
      $scope.sending = true;

      var last = angular.copy($scope.current);
      $scope.current = null;
      $scope.$apply();
      if (last != null) {
        Organization.all("clients").one($scope.clientId).all("room").patch({body: last}).then(function (data) {
          //$scope.current = null;
          addNewMessage(data);
          $scope.sending = false;
          visit();
        }).catch(function () {
          $scope.current = last;
          $scope.sending = false;
        })
      }
    }
  });


angular.module('webappApp').controller('MessageController', function ($scope, Organization) {


  $scope.placeholder = "This message was deleted";


  $scope.owner = $scope.message.sender.name === $scope.currentUser.name;
  $scope.edit = function () {
    $scope.preview = false;
  }

  $scope.timeToChange = function (message) {
    var input = new Date(parseInt(message.date));

    var then = moment(input);

    var now = moment(new Date());

    var difference = moment.duration(now.diff(then))
    return difference._data.minutes < 10;

  }
  $scope.delete = function () {
    $scope.message.body = null;
    $scope.patchMessage();
  }
  $scope.patchMessage = function () {
    Organization.all("clients").one($scope.clientId).all("room").one(encodeURI($scope.message.id.replace("#", ""))).patch({body: $scope.message.body}).then(function (data) {
      $scope.preview = true
    }).catch(function () {
      $scope.preview = true;
    })
  }
});
