define([ 'marionette', 'utils/locale', 'landing/templates' ], function(Marionette, Locale, templates) {
  "use strict";

  return Marionette.ItemView.extend({
    template : templates.serverForm,

    ui : {
      enabled : "#enabled",
      name : "#name",
      url : "#url",
      username : "#username",
      password : "#password"
    },

    triggers : function() {
      var submitType = this.model.isNew() ? "add" : "update";
      return {
        "submit form" : "server:" + submitType
      };
    }
  });
});