define([ 'marionette', 'landing/templates' ], function(Marionette, templates) {
  "use strict";

  // A Grid Row
  var GridRow = Marionette.ItemView.extend({
    template : templates.log,
    tagName : "tr",

    initialize : function(options) {
      Marionette.ItemView.prototype.initialize.call(this, options);
      this.servers = this.options.servers;
    },

    templateHelpers : {
      getServerName : function(id) {
        return this.servers.get(id).get('name');
      }
    },

    serializeData : function() {
      var data = Marionette.ItemView.prototype.serializeData.apply(this, arguments);
      data.servers = this.options.servers;
      return data;
    }
  });

  var NoItemsView = Marionette.ItemView.extend({
    template : templates.noitems
  });

  // The grid view
  return Marionette.CompositeView.extend({
    template : templates.logs,
    itemView : GridRow,
    
    tableOptions : {
      "bPaginate" : false,
      "bFilter" : false,
      "bInfo" : false,
      "aaSorting" : [ [ 0, 'desc' ] ]
    },

    initialize : function(options) {
      Marionette.CompositeView.prototype.initialize.call(this, options);
      this.itemViewOptions = {
        servers : options.servers
      };
      this.extOptions = {
        "bPaginate" : false
      };
    },

    appendHtml : function(collectionView, itemView) {
      collectionView.$("tbody").append(itemView.el);
    }
  });
});