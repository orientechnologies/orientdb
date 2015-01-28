'use strict';

/**
 * @ngdoc function
 * @name webappApp.controller:AboutCtrl
 * @description
 * # AboutCtrl
 * Controller of the webappApp
 */
angular.module('webappApp')
  .controller('ChatCtrl', function ($scope, Organization, $routeParams, $route, User, $timeout, BreadCrumb,$location) {

    $scope.isNew = false;
    $scope.placeholder = "Click here to type a message. Ctrl/Command + Enter to send.";
    $scope.clientId = $routeParams.id;
    $scope.chatService = new WebSocket(WEBSOCKET);

    $scope.chatService.onopen = function () {
      console.log("Connected to chat service! ")

      $scope.$apply(function () {
        $scope.connected = true;
      })
    };

// called when a message received from server
    $scope.chatService.onmessage = function (evt) {
      var msg = JSON.parse(evt.data);

      if (msg.sender.name != $scope.currentUser.name) {
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
        $scope.notify(msg);
      }
    };


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
// called when socket connection closed
    $scope.chatService.onclose = function () {
      console.log("Disconnected from chat service!")
    };

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

      Organization.all("rooms").getList().then(function (data) {
        $scope.clients = data.plain();
        if ($scope.clients.length > 0) {
          if (!$scope.clientId) {
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
        }
        getMessages();
        var msg = {
          "action": "join",
          "rooms": []
        }
        $scope.clients.forEach(function (c) {
          if (!c.timestamp) c.timestamp = 0;
          msg.rooms.push(c.clientId);
        })
        $scope.$watch('connected', function (val) {
          if (val) {
            $scope.chatService.send(JSON.stringify(msg));
          }
        })

      })

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

      $scope.notify = function (msg) {
        if (!("Notification" in window)) {
          alert("This browser does not support desktop notification");
        }

        // Let's check if the user is okay to get some notification
        else if (Notification.permission === "granted") {
          // If it's okay let's create a notification
          var notification = new Notification("Room " + $scope.getClientName(msg.clientId), {body: msg.sender.name + ": " + msg.body});
          notification.onclick = function(){
            $scope.$apply(function(){
              $location.path('rooms/' + msg.clientId);
            })
          }
        }

        // Otherwise, we need to ask the user for permission
        // Note, Chrome does not implement the permission static property
        // So we have to check for NOT 'denied' instead of 'default'
        else if (Notification.permission !== 'denied') {
          Notification.requestPermission(function (permission) {
            // If the user is okay, let's create a notification
            if (permission === "granted") {

              var notification = new Notification("Room " + $scope.getClientName(msg.clientId), {body: msg.body});
            }
          });
        }
      }
    });

    $scope.getClientName = function (clientId) {
      var name = ''
      $scope.clients.forEach(function (c) {
        if (c.clientId == clientId) name = c.name;

      });
      return name;
    }

    $scope.createChat = function () {
      Organization.all("clients").one($scope.clientId).all("room").post().then(function (data) {
        var jacked = humane.create({baseCls: 'humane-jackedup', addnCls: 'humane-jackedup-success'})
        jacked.log("Room created");
        $route.reload();
      });
    }
    $scope.sendMessage = function () {
      Organization.all("clients").one($scope.clientId).all("room").patch({body: $scope.current}).then(function (data) {
        $scope.current = null;
        addNewMessage(data);

      })
    }
  });
