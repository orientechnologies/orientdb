define([ 'marionette' ], function(Marionette) {
  'use strict';

  return Marionette.AppRouter.extend({
    appRoutes : {
      'home' : 'home',
      'servers/:id' : 'profileServer',
      'servers/:id/:db' : 'profileServer',
      'settings/servers' : 'servers',
      'settings/servers/new' : 'addServer',
      'settings/servers/:id' : 'editServer',
      'logout' : 'logout'
    },

    routes : {
      "*defaults" : "defaultRoute"
    },
    defaultRoute : function() {
      window.location.href = '#home';
    }
  });
});
