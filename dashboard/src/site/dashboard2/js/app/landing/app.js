/*global $*/
define(
  ['landing/namespace', 'marionette', 'landing/templates', 'landing/views/base'],
  function(ns, Marionette, templates){
    "use strict";

    var app = ns.app;

    app.addRegions({
      menu   : '#topmenu',
      center   : '#center',
      content   : '#content',
      footer : '#footer'
    });

    app.addInitializer(function(){
      app.menu.show(new Marionette.ItemView({template : templates.menu}));
      app.footer.show(new Marionette.ItemView({template : templates.footer}));
    });

    return app;

  }
);