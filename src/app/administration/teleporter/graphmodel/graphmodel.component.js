"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var core_1 = require("@angular/core");
var static_1 = require("@angular/upgrade/static");
var GraphodelPanelComponent = (function () {
    function GraphodelPanelComponent() {
    }
    return GraphodelPanelComponent;
}());
GraphodelPanelComponent = __decorate([
    core_1.Component({
        selector: 'graph-model-panel',
        templateUrl: "./graphmodelpanel.component.html",
        styleUrls: []
    })
], GraphodelPanelComponent);
exports.GraphodelPanelComponent = GraphodelPanelComponent;
angular.module('ermodelpanel.component', []).directive("er-model-panel", static_1.downgradeComponent({ component: GraphodelPanelComponent }));
