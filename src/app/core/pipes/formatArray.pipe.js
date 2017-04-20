"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var static_1 = require("@angular/upgrade/static");
var core_1 = require("@angular/core");
var FormatArrayPipe = (function () {
    function FormatArrayPipe() {
    }
    FormatArrayPipe.prototype.transform = function (input) {
        if (input instanceof Array) {
            var output = "";
            input.forEach(function (e, idx) {
                output += (idx > 0 ? ", " : " ") + e;
            });
            return output;
        }
        else {
            return input;
        }
    };
    return FormatArrayPipe;
}());
FormatArrayPipe = __decorate([
    core_1.Injectable()
], FormatArrayPipe);
exports.FormatArrayPipe = FormatArrayPipe;
angular.module('legacy.filters', []).factory("FormatArrayPipe", static_1.downgradeInjectable(FormatArrayPipe));
