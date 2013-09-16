define(function(require) {
	"use strict";
	return {
		menu : require('tpl!landing/templates/menu.html'),
		footer : require('tpl!landing/templates/footer.html'),
		queries : require('tpl!landing/templates/queries.html'),
		query : require('tpl!landing/templates/query.html'),
		metrics : require('tpl!landing/templates/metrics.html'),
		servers : require('tpl!landing/templates/servers.html'),
		server : require('tpl!landing/templates/server.html'),
		servers_status : require('tpl!landing/templates/servers_status.html'),
        server_status : require('tpl!landing/templates/server_status.html'),
		serverForm : require('tpl!landing/templates/server_form.html'),
		logs : require('tpl!landing/templates/logs.html'),
		log : require('tpl!landing/templates/log.html'),
		noitems : require('tpl!landing/templates/noitems.html'),
		stats : require('tpl!landing/templates/stats.html'),
		home_layout : require('tpl!landing/templates/home_layout.html'),
		profile_layout : require('tpl!landing/templates/profile_layout.html'),
		welcome : require('tpl!landing/templates/welcome.html')
	};
});
