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
var ProfilerService = (function () {
    function ProfilerService(http) {
        this.http = http;
    }
    ProfilerService.prototype.profilerData = function (params) {
        var url = constants_1.API + 'sqlProfiler/' + params.db;
        return this.http.get(url).toPromise().then(function (data) {
            return data.json();
        });
    };
    ProfilerService.prototype.reset = function (params) {
        var url = constants_1.API + 'sqlProfiler/' + params.db + '/reset';
        return this.http.get(url).toPromise().then(function (data) {
            return data.json();
        });
    };
    ProfilerService.prototype.metadata = function () {
        var url = constants_1.API + 'profiler/metadata';
        return this.http.get(url).toPromise().then(function (data) {
            return data.json();
        });
    };
    ProfilerService.prototype.realtime = function () {
        var url = constants_1.API + 'profiler/realtime';
        return this.http.get(url).toPromise().then(function (data) {
            return data.json();
        });
    };
    return ProfilerService;
}());
ProfilerService = __decorate([
    core_1.Injectable()
], ProfilerService);
exports.ProfilerService = ProfilerService;
angular.module('command.services', []).factory("ProfilerService", static_1.downgradeInjectable(ProfilerService));
