define([ 'backbone' ], function(Backbone) {
	'use strict';
	
	var OBase = Backbone.Model.extend({
		idAttribute : '@rid',
		
		initialize : function(attributes) {
			if(attributes) {
				this.id =  attributes['@rid'] || attributes['id'];
			}
		},
		getEncodedId : function() {
			return encodeURIComponent(this.id);
		}
	});

	var ORecord = OBase.extend({
		urlRoot : function() {
			return '/document/monitor';
		}
	});
	
	var Server = ORecord.extend({
		defaults: {
		    '@class' : 'Server'
		},
		
		deleteLogs : function() {
			$.ajax({
				type : 'POST',
				url : '/command/monitor/sql/delete from Log where server = ' + this.getEncodedId(),
				dataType : 'json',
				contentType : "application/json; charset=utf-8",
				async : false
			});
		},
		
		deleteSnapshots : function() {
			$.ajax({
				type : 'POST',
				url : '/command/monitor/sql/delete from Metric where snapshot.server = ' + this.getEncodedId(),
				dataType : 'json',
				contentType : "application/json; charset=utf-8",
				async : false
			});
			$.ajax({
				type : 'POST',
				url : '/command/monitor/sql/delete from Snapshot where server = ' + this.getEncodedId(),
				dataType : 'json',
				contentType : "application/json; charset=utf-8",
				async : false
			});
		}	
	});
	
	return {
		ORecord : ORecord,
		Server : Server
	};
});