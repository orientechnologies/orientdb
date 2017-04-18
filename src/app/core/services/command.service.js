"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var static_1 = require("@angular/upgrade/static");
require("rxjs/add/operator/toPromise");
var constants_1 = require("../../../constants");
var core_1 = require("@angular/core");
var CommandService = (function () {
    function CommandService(http) {
        this.http = http;
    }
    CommandService.prototype.command = function (params) {
        var startTime = new Date().getTime();
        params.limit = params.limit || 20;
        params.language = params.language || 'sql';
        var url = constants_1.API + 'command/' + params.db + "/" + params.language + "/-/" + params.limit + '?format=rid,type,version,class,graph';
        params.query = params.query.trim();
        return this.http.post(url, params.query).toPromise();
    };
    return CommandService;
}());
CommandService = __decorate([
    core_1.Injectable()
], CommandService);
exports.CommandService = CommandService;
angular.module('command.services', []).factory("CommandService", static_1.downgradeInjectable(CommandService));
