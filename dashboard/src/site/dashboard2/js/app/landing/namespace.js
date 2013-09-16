define(["marionette"],
// Namespace the application to provide a mechanism for having application wide
// code without having to pollute the global namespace
function(Marionette) {
	
	// mimics the hashCode() function of java.lang.String
	String.prototype.hashCode = function() {
	  for(var ret = 0, i = 0, len = this.length; i < len; i++) {
	    ret = (31 * ret + this.charCodeAt(i)) << 0;
	  }
	  return ret;
	};
	
	String.prototype.escapeRegExp = function() {
		return this.replace(/[\-\[\]\/\{\}\(\)\*\+\?\.\\\^\$\|]/g, "\\$&");
	};

    return {
	    // Keep active application instances namespaced under an app object.
        app: new Marionette.Application()
    };
	
});