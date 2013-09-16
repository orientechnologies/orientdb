define([ 'landing/namespace', 'marionette', 'landing/templates' ], function(ns, Marionette, templates) {
  "use strict";

  // A Grid Row
  var GridRow = Marionette.ItemView.extend({
    template : templates.server_status,
    tagName : "tr"
  });

  var NoItemsView = Marionette.ItemView.extend({
    template : templates.noitems
  });

  // The grid view
  return Marionette.CompositeView.extend({
    template : templates.servers_status,
    itemView : GridRow,
    
    tableOptions : {
      "bPaginate" : false,
      "bFilter" : false,
      "bInfo" : false
    },
    
    refresh : 10000
  });
});