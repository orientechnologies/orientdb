import {Component, NgZone, AfterViewChecked} from '@angular/core';

import * as $ from "jquery"

import {downgradeComponent} from '@angular/upgrade/static';
import {TeleporterService} from '../../core/services';

declare var angular:any

@Component({
  selector: 'teleporter',
  templateUrl: "./teleporter.component.html",
  styleUrls: []
})

class TeleporterComponent implements AfterViewChecked {

  private protocols;
  private strategies;
  private nameResolvers;
  private includedClasses;
  private excludedClasses;
  private logLevels;
  private dbConnection;

  private jurlPattern;
  private jurlSplit;

  private defaultConfig;
  private config;
  private step;

  private driversInfo;
  private driverNames;

  private job;
  private jobRunning;

  private hints;

  constructor(private teleporterService:TeleporterService, private zone: NgZone) {

    this.protocols = ["plocal", "remote"];
    this.strategies = ["naive", "naive-aggregate"];
    this.nameResolvers = ["original", "java"];
    this.includedClasses = [];
    this.excludedClasses = [];
    this.logLevels = ["NO","DEBUG","INFO","WARNING","ERROR"];

    this.dbConnection = {
      "host": "",
      "port": "",
      "dbName": "",
      "sid": ""
    }

    this.jurlSplit = [];

    this.defaultConfig = {
      "driver": "PostgreSQL",
      "jurl": "",
      "username": "",
      "password": "",
      "protocol": "plocal",
      "url": "",
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

    this.hints = {
      driver: "Driver name of the DBMS from which you want to execute the migration.",
      host: "Address of the host where the DBMS is available.",
      port: "The port where your DBMS is listening for connections.",
      dbName: "The source database name.",
      sid: "SID is a unique name for an Oracle database instance.",
      username: "The username to access the source database.",
      password: "The password to access the source database.",
      protocol: "The protocol to use during the migration in order to connect to OrientDB:<br>" +
                "<li><b>plocal</b>: the dabase will run locally in the same JVM of your application.</li>" +
                "<li><b>remote</b>: the database will be accessed via TCP/IP connection.</li>",
      outDbUrl: "URL for the destination OrientDB graph database.",
      strategy: "Strategy adopted during the importing phase.<br> " +
                        "<li><b>naive</b>: performs a 'naive' import of the data source. The data source schema is translated semi-directly in a correspondent and coherent graph model.</li> " +
                        "<li><b>naive-aggregate</b>: performs a 'naive' import of the data source. The data source schema is translated semi-directly in a correspondent and coherent graph model " +
                        "using an aggregation policy on the junction tables of dimension equals to 2.</li><br>" +
                        "<a href='http://orientdb.com/docs/last/Teleporter-Execution-Strategies.html'>More info</a>",
      nameResolver: "Name of the resolver which transforms the names of all the elements of the source database according to a specific convention.<br>" +
                          "<li><b>original</b>: maintains the original name convention.</li>" +
                          "<li><b>java</b>: transforms all the elements' names according to the Java convention.</li>",
      includedTables: "Allows you to import only the listed tables.<br><br><a href='http://orientdb.com/docs/last/Teleporter-Import-Filters.html'>More info</a>",
      excludedTables: "Excludes the listed tables from the importing process.<br><br><a href='http://orientdb.com/docs/last/Teleporter-Import-Filters.html'>More info</a>",
      XMLPath: "Executes the migration taking advantage of OrientDB's polymorphism according to the configuration in the specified XML file.<br><br>" +
               "<a href='http://orientdb.com/docs/last/Teleporter-Inheritance.html'>More info</a>",
      logLevel: "Level of verbosity printed to the output during the execution."
    }
  }

  ngAfterViewChecked() {
    this.enablePopovers();
  }

  enablePopovers() {
    (<any>$('[data-toggle="popover"]')).popover({
      title: ' ',
      placement: 'right',
      trigger: 'focus'
    });
  }

  getStep() {
    return this.step;
  }

  switchConfigStep(step) {
    this.step = step;
    //this.enablePopovers();
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

angular.module('teleporter.components', []).directive(
  `teleporter`,
  downgradeComponent({component: TeleporterComponent}));


export {TeleporterComponent};
