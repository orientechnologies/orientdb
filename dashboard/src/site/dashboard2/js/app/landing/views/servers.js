define([ 'landing/namespace', 'marionette', 'utils/locale', 'landing/templates' ], function(ns, Marionette, Locale, templates) {
  "use strict";

  // A Grid Row
  var GridRow = Marionette.ItemView.extend({
    template : templates.server,
    tagName : "tr",

    attributes : function() {
      return {
        "data-row-id" : this.model.id.hashCode()
      };
    },

    events : {
      "click .deleteitem" : "deleteServer"
    },

    deleteServer : function(e) {
      e.preventDefault();
      ns.app.vent.trigger("server:delete", this.model);
    }
  });

  var NoItemsView = Marionette.ItemView.extend({
    template : templates.noitems
  });

  // The grid view
  return Marionette.CompositeView.extend({
    template : templates.servers,
    itemView : GridRow,
    tableOptions : {
      "aaSorting" : [ [ 2, 'asc' ] ]
    },
    refresh : 10000
  });
});