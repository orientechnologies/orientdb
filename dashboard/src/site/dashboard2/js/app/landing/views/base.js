define([ 'marionette', 'utils/locale' ], function(Marionette, Locale) {
  "use strict";

  (function() {
    _.extend(Marionette.View.prototype, {
      initialize : function(options) {
        this.localizer = Locale.getInstance();
      },
      onRender : function() {
        this.localizer.localize(this.el);
        this.handleSelects(this.el);
      },
      handleSelects : function(ctx) {
        var s = $("select", ctx);

        if (s && s.length) {
          require([ 'select2' ], function() {
            s.select2({
              placeholder : "Select an Option...",
              escapeMarkup : function(m) {
                return m;
              },
              width : "resolve"
            });
          });
        }
      }
    });
  }).call(this);

  (function() {
    _.extend(Marionette.ItemView.prototype, {
      serializeData : function() {
        var data = {};

        if (this.model) {
          data.data = this.model.toJSON();
        } else if (this.collection) {
          data.data = {
            items : this.collection.toJSON()
          };
        }

        return data;
      }
    });
  }).call(this);

  (function() {
    _.extend(Marionette.CompositeView.prototype, {
      initialize : function(options) {
        Marionette.View.prototype.initialize.apply(this, options);
        if (this.collection && this.refresh) {
          this.collection.stream({
            interval : this.refresh
          });
        }
      },
      onBeforeRender : function() {
        if (this.tables) {
          this.tables[0].fnClearTable();
        }
      },
      onRender : function() {
        Marionette.View.prototype.onRender.apply(this);
        var self = this;
        require([ 'utils/tables' ], function(dt) {
          if (!self.tables) {
            self.tables = dt.show($("table", self.el), self.tableOptions);
          }
        });
      },
      appendHtml : function(collectionView, itemView) {
        if (this.tables) {
          this.tables[0].fnAddTr(itemView.el);
        } else {
          collectionView.$("tbody").append(itemView.el);
        }
      },
      onItemRemoved : function(item) {
        if (this.tables) {
          this.tables[0].fnDeleteRow(item.el);
        }
      },
      onBeforeClose : function() {
        if (this.refresh) {
          this.collection.unstream();
        }
      }
    });
  }).call(this);
});