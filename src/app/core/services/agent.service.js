"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var static_1 = require("@angular/upgrade/static");
require("rxjs/add/operator/toPromise");
var core_1 = require("@angular/core");
var AgentService = (function () {
    function AgentService(http, profilerService) {
        this.http = http;
        this.profilerService = profilerService;
        this.agent = {
            active: null
        };
    }
    AgentService.prototype.isActive = function () {
        var _this = this;
        if (this.agent.active == null) {
            return this.profilerService.metadata().then(function (data) {
                _this.agent.active = true;
            }).catch(function (err) {
                this.agent.active = false;
            });
        }
        return new Promise(function (resolve, reject) {
            resolve(_this.agent.active);
        });
    };
    return AgentService;
}());
AgentService = __decorate([
    core_1.Injectable()
], AgentService);
exports.AgentService = AgentService;
angular.module('command.services', []).factory("AgentService", static_1.downgradeInjectable(AgentService));
