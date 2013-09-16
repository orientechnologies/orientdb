/**
 * Copyright 2012 NuvolaBase (www.nuvolabase.com)
 */
requirejs.config({
  paths : {
    jquery : [ '//ajax.googleapis.com/ajax/libs/jquery/1.8.2/jquery.min', '//code.jquery.com/jquery-1.8.2.min',
    // If the CDN location fails, load from this location
    '../vendor/jquery-1.8.2' ],
    bootstrap : [ '//netdna.bootstrapcdn.com/twitter-bootstrap/2.1.1/js/bootstrap.min', '../vendor/bootstrap' ],
    datatables : [ '//ajax.aspnetcdn.com/ajax/jquery.dataTables/1.9.4/jquery.dataTables.min', '../vendor/jquery.dataTables' ],
    underscore : '../vendor/lodash',
    backbone : '../vendor/backbone',
    livecollection : '../vendor/LiveCollection',
    marionette : [ '//cdnjs.cloudflare.com/ajax/libs/backbone.marionette/1.0.4-bundled/backbone.marionette.min',
        '../vendor/backbone.marionette' ],
    tpl : '../vendor/tpl',
    i18next : '../vendor/i18next',
    d3 : '../vendor/d3',
    nvd3 : '../vendor/nv.d3',
    fisheye : '../vendor/fisheye',
    daterangepicker : '../vendor/daterangepicker',
    date : '../vendor/date',
    moment : [ '//cdn.jsdelivr.net/momentjs/2.0.0/moment.min', '../vendor/moment' ],
    select2 : '../vendor/select2'
  },
  shim : {
    'datatables' : [ 'jquery' ],
    'i18next' : [ 'jquery' ],
    'bootstrap' : {
      deps : [ 'jquery' ],
      exports : "Bootstrap"
    },
    'backbone' : {
      exports : 'Backbone',
      deps : [ 'jquery', 'underscore' ]
    },
    'marionette' : {
      exports : 'Backbone.Marionette',
      deps : [ 'backbone' ]
    },
    'livecollection' : {
      exports : 'LiveCollection',
      deps : [ 'backbone' ]
    },
    'daterangepicker' : {
      exports : 'daterangepicker',
      deps : [ 'bootstrap', 'date' ]
    },
    'd3' : {
      exports : "d3"
    },
    'nvd3' : {
      deps : [ 'd3', 'fisheye' ],
      exports : 'nv'
    },
    'fisheye' : {
      deps : [ 'd3' ]
    },
    'landing/loaders/nvd3' : {
      deps : [ 'nvd3' ]
    }
  },
  deps : [ 'jquery', 'bootstrap', 'moment' ]
});

/**
 * OrientDB Enterprise landing page entry point
 */
require([ 'landing/app', 'jquery', 'backbone', 'landing/router', 'landing/controller' ], function(app, $, Backbone, Router, Controller) {
  "use strict";

  // $.ajaxSetup({beforeSend: function(jqXHR){
  // jqXHR.setRequestHeader("OAuthentication", "custom");
  // }});

  // $.ajaxSetup({
  // processData: false
  // });

  new Router({
    controller : Controller
  });

  // After application initialization kick off our route handlers.
  app.on("initialize:after", function() {
    Backbone.history.start();
  });
  app.start();
});
