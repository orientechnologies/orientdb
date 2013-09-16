define([ 'jquery', 'backbone', 'landing/namespace', 'landing/views/home', 'landing/views/servers', 'landing/views/server',
    'landing/views/servers_status', 'landing/views/logs', 'landing/views/stats', 'landing/views/profile_layout', 'landing/views/queries',
    'landing/views/metrics', 'landing/collections', 'landing/models' ], function($, Backbone, ns, HomeLayout, ServersView, ServerFormView,
    ServersStatusView, LogsView, StatsView, ProfileLayout, QueriesView, MetricsView, Collections, Models) {
  "use strict";

  return {
    home : function() {

      var servers = new Collections.Servers();
      servers.fetch({
        success : function() {
          var home = new HomeLayout();
          ns.app.content.show(home);
          if (servers && servers.length) {

            home.northwest.show(new StatsView({
              collection : servers
            }));
            home.northeast.show(new ServersStatusView({
              collection : servers
            }));

            var logs = new Collections.Logs({}, {
              'limit' : 10
            });
            logs.fetch({
              success : function() {
                home.south.show(new LogsView({
                  collection : logs,
                  servers : servers
                }));
              }
            });
          } else {
            require([ 'landing/views/welcome' ], function(WelcomeView) {
              ns.app.content.show(new WelcomeView());
            });
          }
        }
      });

    },
    profileServer : function(serverId, dbName) {

      var servers = new Collections.Servers(null, {
        filters : {
          status : 'ONLINE'
        }
      });
      servers.fetch({
        async : false
      });

      if (servers && servers.length) {
        var serverName, commands;
        if (serverId) {
          serverName = servers.get(serverId).get('name');
        } else {
          serverName = servers.at(0).get('name');
          serverId = servers.at(0).id;
        }

        var dbs = new Collections.RealtimeMetrics({}, {
          server : serverName,
          kind : 'information',
          name : 'system.databases'
        });
        dbs.fetch({
          async : false
        });

        if (!dbName) {
          if (dbs.length) {
            dbName = dbs.at(0).get('value').split(',')[0];
          }
        }

        if (dbName) {
          var url = 'servers/' + serverId + (dbName ? '/' + dbName : '');
          Backbone.Router.prototype.navigate.call(this, url, {
            replace : true
          });

          // temporary: realtime service does not support inner db folders
          var dbSimpleName = dbName.indexOf('$') >= 0 ? dbName.substring(dbName.lastIndexOf('$') + 1) : dbName;

          commands = new Collections.RealtimeMetrics(null, {
            server : serverName,
            kind : 'chrono',
            name : 'db.' + dbSimpleName + ".command."
          });

          var layout = new ProfileLayout({
            servers : servers,
            databases : dbs,
            serverId : serverId,
            database : dbName
          });

          layout.on("profiler:serverChanged", function() {
            var url = 'servers/' + layout.ui.selectServer.val();
            Backbone.Router.prototype.navigate.call(this, url, {
              trigger : true
            });
          });
          layout.on("profiler:databaseChanged", function() {
            var url = 'servers/' + serverId + '/' + layout.ui.selectDatabase.val();
            Backbone.Router.prototype.navigate.call(this, url, {
              trigger : true
            });
          });

          ns.app.content.show(layout);

          // queries
          commands.fetch({
            success : function() {
              var qView = new QueriesView({
                collection : commands,
                serverId : serverId,
                database : dbName
              });
              qView.on("queryprofiler:refresh", function() {
                commands.fetch();
              });
              qView.on("queryprofiler:clean", function() {
                commands.clean();
                commands.fetch();
              });

              layout.queriestab.show(qView);
            }
          });

          layout.on("profiler:metricstab", function(tab) {
            // metrics
            var dictionary = new Collections.Dictionary();
            dictionary.fetch({
              success : function() {
                // filters only chrono without *
                var chronos = dictionary.filter(function(metric) {
                  return metric.get("type") === "CHRONO" && metric.get('name').indexOf('*') < 0;
                });

                if (chronos.length) {
                  var metricName = chronos[0].get('name');
                  var metrics = new Collections.Metrics(null, {
                    fields : 'snapshot.dateTo as dateTo, name, entries, last, min, max, average, total',
                    filters : {
                      name : metricName,
                      'snapshot.server' : servers.get(serverId).getEncodedId()
                    }
                  });

                  metrics.fetch({
                    success : function() {
                      var mView = new MetricsView({
                        collection : metrics,
                        dictionary : chronos,
                        metric : metricName
                      });
                      
                      mView.on("metrics:metricchanged", function() {
                        metrics.filters.name = mView.ui.selectMetric.val();
                        metrics.fetch({
                          success : function() {
                            mView.collection = metrics;
                            mView.options.metric = metrics.filters.name;
                            layout.metricstab.show(mView);
                          }
                        });
                      });
                      
                      layout.metricstab.show(mView);
                    }
                  });
                }
              }
            });
          });
        }
      }
    },
    servers : function() {
      var servers = new Collections.Servers();
      var sView = new ServersView({
        collection : servers
      });

      ns.app.vent.on('server:delete', function(server) {
        // server.save({enabled : false}, {async: false});
        server.destroy({
          async : false
        });
        server.deleteLogs();
        server.deleteSnapshots();
      });

      ns.app.content.show(sView);
    },
    addServer : function() {
      var server = new Models.Server({
        enabled : true
      });
      var sView = new ServerFormView({
        model : server
      });

      sView.on("server:add", function() {
        server.set({
          enabled : sView.ui.enabled.is(':checked'),
          name : sView.ui.name.val(),
          url : sView.ui.url.val(),
          user : sView.ui.username.val(),
          password : sView.ui.password.val()
        });
        server.save(null, {
          complete : function(xhr) {
            if (xhr.readyState == 4) {
              if (xhr.status == 201) {
                window.location.href = '#settings/servers';
              }
            } else {
              alert('error ' + resp);
            }
          }
        });
      });

      ns.app.content.show(sView);
    },
    editServer : function(id) {
      var server = new Models.Server({
        id : id
      });
      server.fetch({
        async : false
      });
      var sView = new ServerFormView({
        model : server
      });

      sView.on("server:update", function() {
        server.set({
          enabled : sView.ui.enabled.is(':checked'),
          name : sView.ui.name.val(),
          url : sView.ui.url.val(),
          user : sView.ui.username.val(),
          password : sView.ui.password.val()
        });
        server.save(null, {
          complete : function(xhr) {
            if (xhr.readyState == 4) {
              if (xhr.status == 200) {
                window.location.href = '#settings/servers';
              }
            } else {
              alert('error ' + resp);
            }
          }
        });
      });

      ns.app.content.show(sView);
    },
    logout : function() {
      $.ajax({
        url : '/disconnect',
        complete : function() {
          location.href = '../home/';
        }
      });
    }
  };
});