"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var static_1 = require("@angular/upgrade/static");
require("rxjs/add/operator/toPromise");
var _ = require("underscore");
var core_1 = require("@angular/core");
var NotificationService = (function () {
    function NotificationService() {
    }
    NotificationService.prototype.push = function (notification) {
        var n;
        if (this.current) {
            this.current.close();
        }
        if (notification.error) {
            if (typeof notification.content != 'string') {
                notification.content = notification.content.errors[0].content;
            }
            n = noty({ text: _.escape(notification.content), layout: 'bottom', type: 'error', theme: 'relax' });
        }
        else if (notification.warning) {
            n = noty({ text: notification.content, layout: 'bottom', type: 'warning', theme: 'relax' });
        }
        else {
            n = noty({ text: notification.content, layout: 'bottom', type: 'success', theme: 'relax' });
        }
        this.current = n;
        this.attachEvents();
        this.timerID = setTimeout(function () {
            if (n && !(n.options.type === 'error') && !(notification.sticky))
                n.close();
        }, 4000);
    };
    NotificationService.prototype.attachEvents = function () {
        var _this = this;
        $('[class="noty_message"]').on({
            mouseover: function () {
                _this.stopTimer();
            },
            mouseleave: function () {
                _this.startTimer();
            }
        });
    };
    NotificationService.prototype.startTimer = function () {
        var _this = this;
        this.timerID = setTimeout(function () {
            _this.clear();
        }, 4000);
    };
    NotificationService.prototype.stopTimer = function () {
        if (this.timerID) {
            clearTimeout(this.timerID);
        }
    };
    NotificationService.prototype.clear = function () {
        if (this.current) {
            this.current.close();
        }
    };
    return NotificationService;
}());
NotificationService = __decorate([
    core_1.Injectable()
], NotificationService);
exports.NotificationService = NotificationService;
angular.module('command.services', []).factory("NotificationService", static_1.downgradeInjectable(NotificationService));
