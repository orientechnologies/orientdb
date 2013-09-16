/*global window */
/**
 * Copyright 2012 NuvolaBase (www.nuvolabase.com)
 */
requirejs.config({
  paths : {
    jquery : [ '//ajax.googleapis.com/ajax/libs/jquery/1.8.2/jquery.min', '//code.jquery.com/jquery-1.8.2.min',
    // If the CDN location fails, load from this location
    '../vendor/jquery-1.8.2' ],
    Bootstrap : [ '//netdna.bootstrapcdn.com/twitter-bootstrap/2.1.1/js/bootstrap.min', '../vendor/bootstrap' ],
    underscore : '../vendor/lodash',
    backbone : '../vendor/backbone',
    i18next : '../vendor/i18next',
    select2 : '../vendor/select2',
    messenger : "../vendor/messenger",
    messengerTheme : "../vendor/messenger-theme-future"
  },
  shim : {
    'backbone' : [ 'underscore', 'jquery' ],
    'i18next' : [ 'jquery' ],
    'Bootstrap' : {
      deps : [ 'jquery' ],
      exports : "Bootstrap"
    },
    'messenger' : {
      deps : [ 'jquery' ],
      exports : 'Messenger'
    },
    'messengerTheme' : [ 'messenger' ]
  }
});

/**
 * OrientDB Enterprise home page entry point with login handler
 */
require([ 'jquery', 'underscore', 'utils/locale', 'utils/ui' ], function($, _, Locale, UI) {
  'use strict';

  var init = function() {

    var localizer = Locale.getInstance();
    localizer.localize();

    // user type radios handling
    $('input[name="userType"]').change(function() {
      if ($("input[name='userType']:checked").val() == 'admin'){
        $('#databases').slideUp();
      } else if ($('input[name="userType"]:checked').val() == 'dbuser'){
        $('#databases').slideDown();
      }
    });

    $('#loginform').submit(function(e) {
      e.preventDefault();
      $.ajax({
        url : '/connect/monitor',
        dataType : 'json',
        username : $('#username').val(),
        password : $('#password').val(),
        success : onSuccess,
        error : onError
      });
    });

    $('input:text:visible:first').focus();
  };

  var onSuccess = function(response, statusText, xhr) {
    var view = window.location.hash ? window.location.hash : '#home';
    window.location.href = '../landing/' + view;
  };
  var onError = function(xhr, e, status) {
    UI.showAlert(Locale.getInstance().getText('systemerror'), UI.ALERT_ERROR);
  };

  init();
});