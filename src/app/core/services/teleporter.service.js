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
var TeleporterService = (function () {
    function TeleporterService(http) {
        this.http = http;
    }
    TeleporterService.prototype.drivers = function () {
        var url = constants_1.API + 'teleporter/drivers';
        return this.http.get(url).toPromise().then(function (data) {
            return data.json();
        });
    };
    TeleporterService.prototype.launch = function (params) {
        var url = constants_1.API + 'teleporter/job';
        return this.http.post(url, params).toPromise().then(function (data) {
            return data.json();
        });
    };
    TeleporterService.prototype.testConnection = function (params) {
        var url = constants_1.API + 'teleporter/test';
        return this.http.post(url, params).toPromise().then(function (data) {
            return data.json();
        });
    };
    TeleporterService.prototype.status = function () {
        var url = constants_1.API + 'teleporter/status';
        return this.http.get(url).toPromise().then(function (data) {
            return data.json();
        });
    };
    TeleporterService.prototype.getTablesNames = function (params) {
        var url = constants_1.API + 'teleporter/tables';
        return this.http.post(url, params).toPromise().then(function (data) {
            return data.json();
        });
    };
    return TeleporterService;
}());
TeleporterService = __decorate([
    core_1.Injectable()
], TeleporterService);
exports.TeleporterService = TeleporterService;
angular.module('command.services', []).factory("TeleporterService", static_1.downgradeInjectable(TeleporterService));
