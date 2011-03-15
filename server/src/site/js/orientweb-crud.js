function OCRUD(className, options) {
    this.className = className;
    this.objId;
    this.options;
    this.configFileName = 'www/configFile.js';

    var crud = this;
    if (typeof options == 'string' || !options) {
        if (typeof options == 'string') 
            this.configFileName = options;

        $.getJSON(this.configFileName, function(data) {
        	if (!options)
        		options = {};
            $.each(data, function(key, val) {
    			options[key] = val;
  			});
  			crud.init(options);
        });
    } else {
        this.init(options);
    }
}

OCRUD.prototype.init = function(options) {
	this.options = options;
	var crud = this;
	
    if (!window.database) {
        window.database = new ODatabase(options['databaseURL']);
        window.databaseInfo = database.open();
        window.user = null;
    }
    if (!window.form) {
        window.form = new OForm({debug : false});
    }
    if (!window.controller) {
        window.controller = new OController({component : this.className + "-container", rewriteUrl : true});
    }
    
   	$("#"+this.className).hide();
   	
   	var popup = $("#" + this.className + "-deletePopup");
   	if (popup.length) {
   		popup.hide();
   		
	   	popup.find("#" + this.className + "-deletePopupYes").click(function(event) {
			var myobj = form.object[crud.objId];
			if (myobj) {
				database.remove(myobj['@rid'], function() {
					crud.read();
				});
			}
			popup.hide();
		});
		popup.find("#" + this.className + "-deletePopupNo").click(function(event) {
			popup.hide();
		});
   	}
   	
   	$(document.getElementById(this.className + "!search")).click(function() {
   		crud.read(crud);
   	});
   	$(document.getElementById(this.className + "!create")).click(function() {
  		crud.create();
   	});
}

OCRUD.prototype.getDatabase = function() {
	if (!window.database) {
        window.database = new ODatabase(options['databaseURL']);
        window.databaseInfo = database.open();
        window.user = null;
    }
    return window.database;
}

OCRUD.prototype.getForm = function() {
	if (!window.form) {
        window.form = new OForm({debug : false});
    }
	return window.form;
}

OCRUD.prototype.getController = function(newComponent) {
	var componentName = (newComponent) ? newComponent : this.className + "-container"; 
	
	if (!window.controller) {
        window.controller = new OController({component : this.className + "-container", rewriteUrl : true});
    } else if (newComponent) {
   		window.controller.component = newComponent;
    }
	return window.controller;
}

OCRUD.prototype.getQuery = function() {
	var query = "select from " + this.className;
	var val = document.getElementById(this.className + "!filter").value;
	
	if (val) 
		query += " where any() = '" + val + "'";
	return query;
}

OCRUD.prototype.create = function() {
	window.controller.parameter("rid", null);
	window.controller.loadFragment(this.options["entityPages"][this.className]);
}

OCRUD.prototype.read = function(crud) {
	var queryResult = window.database.query(this.getQuery(), -1, "");
	
	if (queryResult["result"]) {
		form.object = null;
		form.bindArray2component(queryResult["result"], this.className);
		
		var updateExp = new RegExp(this.className+"_[0-9]+\!update");
		var deleteExp = new RegExp(this.className+"_[0-9]+\!delete");
		
		$("#"+this.className).find("button[id]").each(function(i, el) {
			if (el.id.match(updateExp)) {
				$(el).click(function(event) {
					event.preventDefault();
					var num = el.id.substring(window.crud.className.length+1, this.id.length-7);
					window.crud.update(parseInt(num));
				});
			} else if (el.id.match(deleteExp)) {
				$(el).click(function(event) {
					event.preventDefault();
					var num = el.id.substring(window.crud.className.length+1, this.id.length-7);
					window.crud.deleteObj(parseInt(num));
				});
			}
		});
		
		$("#"+this.className+":hidden").show();
	}
}

				
OCRUD.prototype.update = function(obj) {
	var myobj = form.object[obj];
	window.controller.parameter("rid", myobj['@rid']);
	window.controller.loadFragment(this.options["entityPages"][this.className]);
}

OCRUD.prototype.deleteObj = function(obj) {
	this.objId = obj;
	$("#" + this.className + "-deletePopup").show();
}		
