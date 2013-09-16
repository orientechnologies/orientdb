define([ 'marionette', 'utils/locale', 'landing/templates' ], function(Marionette, Locale, templates) {
  "use strict";

  return Marionette.Layout.extend({
    template : templates.home_layout,
    regions : {
      northwest : "#north-west",
      northeast : "#north-east",
      south : "#south"
    }
  });
});