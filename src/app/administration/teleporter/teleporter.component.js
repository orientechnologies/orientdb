import {Component, NgZone} from '@angular/core';
import {PopoverModule} from "ngx-popover";

import {downgradeComponent} from '@angular/upgrade/static';
import {TeleporterService} from '../../core/services';

import template from './teleporter.component.html';

class TeleporterComponent {

  constructor(TeleporterService, ngZone) {

    this.zone = ngZone;

    this.teleporterService = TeleporterService;

    this.protocols = ["plocal", "remote"];
    this.strategies = ["naive", "naive-aggregate"];
    this.nameResolvers = ["original", "java"];
    this.includedClasses = [];
    this.excludedClasses = [];
    this.logLevels = ["NO","DEBUG","INFO","WARNING","ERROR"];

    this.dbConnection = {
      "host": "localhost",
      "port": "5432",
      "dbName": "dvdrental",
      "sid": ""
    }

    this.jurlPattern;
    this.jurlSplit = [];

    this.defaultConfig = {
      "driver": "PostgreSQL",
      "jurl": "",
      "username": "postgres",
      "password": "postgres",
      "protocol": "plocal",
      "url": "/Users/gabriele/orientdb-enterprise-2.2.16/databases/DVDRental",
      "outDbUrl": "",
      "strategy": "naive",
      "mapper": "basicDBMapper",
      "xmlPath": "",
      "nameResolver": "original",
      "level": "2",
      "includedTables": [],
      "excludedTables": []
    }

    this.config = angular.copy(this.defaultConfig);

    this.step = '1';

    // fetching driver name and jurl pattern
    this.drivers().then((data) => {
      this.driversInfo = data;
      this.driverNames = Object.keys(this.driversInfo);
      this.jurlPattern = this.driversInfo[this.config.driver].format[0];
      this.updateJurlSplit();
      this.updateJurl();
    });

    this.job = {};
    this.jobRunning = false;

    this.enablePopovers();

  }

  ngOnInit() {
    $('[data-toggle="popover"]').popover();
  }

  enablePopovers() {
    $('[data-toggle="popover"]').popover();
  }

  getStep() {
    return this.step;
  }

  switchConfigStep(step) {
    this.step = step;
  }

  drivers() {
    return this.teleporterService.drivers();
  }

  changeJurlAccordingToDriver() {
    this.jurlPattern = this.driversInfo[this.config.driver].format[0];
    this.updateJurlSplit();
    this.updateJurl();
  }

  updateJurlSplit() {
    var regExp = new RegExp(/(<HOST>|<PORT>|<DB>|<SID>)/);
    this.jurlSplit = this.jurlPattern.split(regExp, 6);
  }

  updateJurl() {
    this.jurlSplit[1] = this.dbConnection.host;
    this.jurlSplit[3] = this.dbConnection.port;
    if(this.config.driver == "Oracle") {
      this.jurlSplit[5] = this.dbConnection.sid;
    }
    else {
      this.jurlSplit[5] = this.dbConnection.dbName;
    }
    this.config.jurl = this.jurlSplit.join("");
  }

  testConnection() {
    this.teleporterService.test(this.config).then(function (data) {
      alert("Connection is alive");
      //Notification.push({content: "Connection is alive", autoHide: true});
    }).catch(function (error) {
      alert("Error!");
      //Notification.push({content: error.data, error: true, autoHide: true});
    });
  }

  launch() {

    this.config.outDbUrl = this.config.protocol + ":" + this.config.url;

    this.teleporterService.launch(this.config).then((data) => {
      this.step = "running";
      this.jobRunning = true;
      this.status();
    }).catch(function (error) {
      alert("Error during migration!")
    });
  }

  status() {
    console.log("Status call.")
    if(this.jobRunning) {
      this.teleporterService.status().then((data) => {
        if (data.jobs.length > 0) {
          this.job = data.jobs[0];
          this.scrollLogAreaDown();
        }
        else {
          if (this.job) {
            this.job.finished = true;
            this.jobRunning = false;
            this.scrollLogAreaDown();
          }
        }

        // start status again after 3 secs
        setTimeout(() => {
          this.zone.run(() => {
            this.status();
          })
        },3000);

      });
    }
  }

  scrollLogAreaDown() {
    console.log("ScrollDown");
    var logArea = $("#logArea");
    logArea.scrollTop(9999999);
  }

}

TeleporterComponent.parameters = [TeleporterService, NgZone];

TeleporterComponent.annotations = [new Component({
  selector: "teleporter",
  template: template,
  inputs: ["message"]
})]


angular.module('teleporter.components', []).directive(
  `teleporter`,
  downgradeComponent({component: TeleporterComponent}));


export {TeleporterComponent};
