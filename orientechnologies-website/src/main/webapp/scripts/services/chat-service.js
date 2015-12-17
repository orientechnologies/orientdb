'use strict';


angular.module('webappApp').factory("ChatService", function ($rootScope, $location, $timeout, $window, $q, User, Organization) {


  var favicon = new Favico({
    animation: 'popFade'
  });

  var charSocketWrapper = {
    socket: null,
    init: function (callback) {

      try {
        this.socket = new WebSocket(WEBSOCKET);
      } catch (err) {
        console.log(err);
        return;
      }
      return callback();
    },
    send: function (msg) {
      this.socket.send(msg);
    },
    deinit: function () {
      this.socket.close();
      this.socket = null;
    }
  }
  var chatService = {
    connected: false,
    polling: false,
    clients: [],
    badge: 0,
    clean: function () {
      this.badge = 0;
      favicon.badge(0);
    },
    notify: function (msg) {
      if (!("Notification" in window)) {
        alert("This browser does not support desktop notification");
      }

      // Let's check if the user is okay to get some notification
      else if (Notification.permission === "granted") {
        // If it's okay let's create a notification
        var notification = new Notification("Room " + this.getClientName(msg.clientId), {body: msg.sender.name + ": " + msg.body});
        notification.onclick = function () {
          $location.path('rooms/' + msg.clientId);
        }
      }

      // Otherwise, we need to ask the user for permission
      // Note, Chrome does not implement the permission static property
      // So we have to check for NOT 'denied' instead of 'default'
      else if (Notification.permission !== 'denied') {
        Notification.requestPermission(function (permission) {
          // If the user is okay, let's create a notification
          if (permission === "granted") {

            var notification = new Notification("Room " + this.getClientName(msg.clientId), {body: msg.body});
          }
        });
      }
    },
    send: function (msg) {
      charSocketWrapper.send(msg);
    },

    connect: function () {

      charSocketWrapper.init(initializer)
      this.polling = true;
    },
    disconnect: function () {
      this.polling = false;
      charSocketWrapper.deinit();
    },
    getClientName: function (clientId) {
      var name = ''
      this.clients.forEach(function (c) {
        if (c.clientId == clientId) name = c.name;
      });
      return name;
    }

  }

  var poll = function () {
    $timeout(function () {
      if (!chatService.connected && chatService.polling) {
        console.log("Reconnecting to chat service! ")
        var q = charSocketWrapper.init(initializer)
        if (q) {
          q.then(function () {
            $rootScope.$broadcast('connection-acquired');
          });
        }

      } else {
        if (chatService.polling) {
          var msg = {"action": "heartbeat"};
          if (charSocketWrapper.socket) {
            charSocketWrapper.socket.send(JSON.stringify(msg));
          }
        }
      }
      poll();
    }, 10000);
  }

  $window.onfocus = function () {
    chatService.clean();
  }
  function initializer() {

    var deferred = $q.defer();
    charSocketWrapper.socket.onopen = function () {
      console.log("Connected to chat service! ")
      chatService.connected = true;

      deferred.resolve();
      User.whoami().then(function (data) {
        chatService.currentUser = data;
        Organization.all("rooms").getList().then(function (data) {
          chatService.clients = data.plain();
          var msg = {
            "action": "join",
            "rooms": []
          }
          chatService.clients.forEach(function (c) {
            if (!c.timestamp) c.timestamp = 0;
            msg.rooms.push(c.clientId);
          })
          chatService.send(JSON.stringify(msg));
        });
      })
    };
    charSocketWrapper.socket.onmessage = function (evt) {
      var msg = JSON.parse(evt.data);
      if (msg.sender.name != chatService.currentUser.name) {
        if (!msg.edited)
          chatService.notify(msg);
        chatService.badge += 1;
        favicon.badge(chatService.badge);
        $rootScope.$broadcast('msg-received', msg);
      }
    };
    charSocketWrapper.socket.onerror = function (err) {
      console.log(err);
    }

    charSocketWrapper.socket.onclose = function (status) {


      if (status instanceof  CloseEvent) {
        
        if (status.code === 1000) {
          console.log("Disconnected from chat service!")
          chatService.connected = false;
        } else {
          chatService.connected = false;
          console.log("Connection broken from chat service!")
          $rootScope.$broadcast('connection-lost');
        }
      }
    };

    return deferred.promise;
  }

  poll();
  return chatService;
}).run(function (ChatService) {

});



