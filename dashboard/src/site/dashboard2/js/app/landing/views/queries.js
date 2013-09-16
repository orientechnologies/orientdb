define([ 'marionette', 'landing/templates' ], function(Marionette, templates) {
  "use strict";

  // A Grid Row
  var GridRow = Marionette.ItemView.extend({
    template : templates.query,
    tagName : "tr"
  });

  var NoItemsView = Marionette.ItemView.extend({
    template : templates.noitems
  });

  // The grid view
  return Marionette.CompositeView.extend({
    template : templates.queries,
    itemView : GridRow,
    tableOptions : {
      "aaSorting" : [ [ 3, 'desc' ] ]
    },
    
    triggers : {
      'click .btn-refresh' : 'queryprofiler:refresh',
      'click .btn-clean' : 'queryprofiler:clean'
    }
    
  });
});