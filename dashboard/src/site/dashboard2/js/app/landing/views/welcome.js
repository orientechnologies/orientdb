define([ 'marionette', 'landing/templates' ], function(Marionette, templates) {
  "use strict";

  return Marionette.ItemView.extend({
    template : templates.welcome,
  });
});