"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var static_1 = require("@angular/upgrade/static");
var core_1 = require("@angular/core");
var FormatErrorPipe = (function () {
    function FormatErrorPipe() {
    }
    FormatErrorPipe.prototype.transform = function (input) {
        if (typeof input == 'string') {
            return input;
        }
        else if (typeof input == 'object') {
            return input.errors[0].content;
        }
        else {
            return input;
        }
    };
    return FormatErrorPipe;
}());
FormatErrorPipe = __decorate([
    core_1.Injectable()
], FormatErrorPipe);
exports.FormatErrorPipe = FormatErrorPipe;
angular.module('legacy.filters', []).factory("FormatErrorPipe", static_1.downgradeInjectable(FormatErrorPipe));
