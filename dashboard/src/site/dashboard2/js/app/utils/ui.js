define([ 'messenger', 'messengerTheme' ], function(Messenger) {

  var loadStylesheet = function(url) {
    if (document.createStyleSheet) {
      try {
        document.createStyleSheet(url);
      } catch (e) {
      }
    } else {
      var css;
      css = document.createElement('link');
      css.rel = 'stylesheet';
      css.type = 'text/css';
      css.media = "all";
      css.href = url;
      document.getElementsByTagName("head")[0].appendChild(css);
    }
  };

  var ALERT_SUCCESS = "success";
  var ALERT_ERROR = "error";
  
  Messenger.options = {
    extraClasses : 'messenger-fixed messenger-on-top messenger-on-left',
    theme : 'future'
  };

  var showAlert = function(msg, type) {
    Messenger().post({
      message : msg,
      type : type,
      showCloseButton : true,
      hideOnNavigate : true
    });
  };

  return {
    loadStylesheet : loadStylesheet,
    ALERT_ERROR : ALERT_ERROR,
    ALERT_SUCCESS : ALERT_SUCCESS,
    showAlert : showAlert
  };
});