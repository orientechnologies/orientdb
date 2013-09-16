/**
 * Copyright 2012 NuvolaBase (www.nuvolabase.com)
 * 
 * Locale Manager singleton implementation. Provides simple methods to localize
 * document contexts and single texts.
 */
define([ 'underscore', 'i18next' ], function(_, i18n) {

  var instance = null;

  function locale() {
    if (instance !== null) {
      throw new Error("Cannot instantiate more than one LocaleManager");
    }
    this.initialize();
  }

  locale.prototype = {

    /**
     * Initialization stuff. TO-DO: handle language in cookie
     */
    initialize : function() {
      i18n.init({
        fallbackLng : 'en-US',
        useLocalStorage : false,
        getAsync : false
      }, function(t) { // will init i18n with default settings and set language
        // from navigator

      });
    },
    /**
     * Localizes a specific area of the document identified by the context
     * parameter. If context is null all the document is localized
     */
    localize : function(context) {
      var self = this;
      $('[data-i18n]', context).each(function(index, elem) {
        var element = $(this);
        var placeholder = element.data('i18n-plhd');
        // if data-i18n-attr exists use it to set text
        var dataAttr = element.data('i18n-attr');
        var text = self.getText(element.data('i18n'), placeholder);
        if (dataAttr) {
          element.attr(dataAttr, text);
        } else if (element.is(":input") && !element.is(":button")) {
          element.val(text);
        } else {
          element.html(text);
        }
      });
    },
    /**
     * Returns the localized text identified by a specific key. If supported
     * includes the given placeholder
     */
    getText : function(key, placeholder) {
      if (!_.isUndefined(placeholder) && _.isString(placeholder)) {
        placeholder = $.parseJSON(placeholder);
      }
      var text = i18n.t(key, placeholder);
      if (text === key && key.indexOf(".") != -1) {
        var endKey = key.substr(key.lastIndexOf(".") + 1);
        var startKey = key.substr(0, key.lastIndexOf("."));
        var subKey;
        if (startKey.indexOf(".") != -1) {
          subKey = (startKey.substr(0, startKey.lastIndexOf(".") + 1)) + endKey;
        } else {
          subKey = endKey;
        }
        text = this.getText(subKey, placeholder);
      }
      return text;
    }
  };

  locale.getInstance = function() {
    if (instance === null) {
      instance = new locale();
    }
    return instance;
  };
  return locale;

});