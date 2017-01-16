import {Component} from '@angular/core';
import {downgradeComponent} from '@angular/upgrade/static';
import {TeleporterService} from '../../core/services';

import template from './teleporter.component.html';

class TeleporterComponent {

  constructor(TeleporterService) {
    this.teleporterService = TeleporterService;

    this.defaultConfig = {
      "driver": "PostgreSQL",
      "jurl": "jdbc:postgresql://<HOST>:<PORT>/<DB>",
      "username": "",
      "password": "",
      "outDbUrl": "",
      "strategy": "naive",
      "mapper": "basicDBMapper",
      "xmlPath": "",
      "nameResolver": "original",
      "level": "2",
      "includes": [],
      "excludes": []
    }

    this.config = angular.copy(this.defaultConfig);
  }

  drivers() {
    this.teleporterService.drivers();
  }

  testConnection() {
    this.teleporterService.test(this.config);
  }

  launch() {
    this.teleporterService.launch(this.config);
  }
}

TeleporterComponent.parameters = [[TeleporterService]];

TeleporterComponent.annotations = [new Component({
  selector: "teleporter",
  template: template
})]


angular.module('teleporter.components', []).directive(
  `teleporter`,
  downgradeComponent({component: TeleporterComponent}));


export {TeleporterComponent};
