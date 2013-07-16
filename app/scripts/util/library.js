

if (!String.prototype.startsWith) {
  Object.defineProperty(String.prototype, 'startsWith', {
    enumerable: false,
    configurable: false,
    writable: false,
    value: function (searchString, position) {
      position = position || 0;
      return this.indexOf(searchString, position) === position;
    }
  });
}

Utilities = {};


Utilities.confirm = function($scope,$dialog,params){

	var title = params.title;
	var msg = params.body;
	var btns = [{result:'cancel', label: 'Cancel'}, {result:'ok', label: 'OK', cssClass: 'btn-primary'}];
	var d = $dialog.messageBox(title, msg, btns);
	d.open().then(function(result){
		if(result ==  'ok')
		{
			params.success();
		}
	});
}

