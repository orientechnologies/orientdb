import {Component, NgZone, AfterViewChecked} from '@angular/core';

import * as $ from "jquery"

import {downgradeComponent} from '@angular/upgrade/static';
import {NotificationService} from "../../core/services/notification.service";
import {AgentService} from "../../core/services/agent.service";
import {Neo4jImporterService} from "../../core/services/neo4jimporter.service"

declare var angular:any

@Component({
  selector: 'neo4jimporter',
  templateUrl: "./neo4jimporter.component.html",
  styleUrls: []
})

class Neo4jImporterComponent implements AfterViewChecked {

  private protocols;
  private dbConnection;
  private logLevels;

  private urlPattern = "bolt://<HOST>:<PORT>";
  private urlSplit;

  private defaultConfig;
  private config;
  private step;

  private job;
  private jobRunning;

  private hints;

  constructor(private neo4jImorterService: Neo4jImporterService, private notification: NotificationService,
              private agentService: AgentService, private zone: NgZone) {

    // agent
    this.agentService.isActive().then(() => {
      this.init();
    });

  }

  init() {

    this.protocols = ["plocal", "memory"];
    this.logLevels = ["NO","DEBUG","INFO","WARNING","ERROR"];

    this.dbConnection = {
      "host": "localhost",
      "port": "7687"
    }

    this.defaultConfig = {
      "neo4jUrl": "",
      "neo4jUsername": "neo4j",
      "neo4jPassword": "admin",
      "odbProtocol": "plocal",
      "odbName": "neo4jImport",
      "outDbUrl": "",
      "overwriteDB": false,
      "indexesOnRelationships": false,
      "logLevel": 2
    }

    this.config = angular.copy(this.defaultConfig);
    this.step = '1';

    this.hints = {
      host: "Address of the host where the neo4j server is available.",
      port: "The port where your neo4j server is listening for new connections.",
      neo4jUsername: "The username to access neo4j server.",
      neo4jPassword: "The password to access neo4j server.",
      protocol: "The protocol to use during the migration in order to connect to OrientDB:<br>" +
      "<li><b>plocal</b>: the dabase will run locally in the same JVM of your application.</li>" +
      "<li><b>remote</b>: the database will be accessed via TCP/IP connection.</li>",
      outDbUrl: "The full path to a directory where the Neo4j database will be migrated. The directory will be created by the import tool. " +
      "In case the directory exists already, the Neo4j to OrientDB Importer will behave accordingly to the checkbox below.",
      overwriteOrientDB: "Overwrite OrientDB target database if it already exists.",
      createIndicesOnRelationhips: "Create indices on imported edges in OrientDB. In this way an index will be built for each Edge class on 'Neo4jRelID' property.",
      logLevel: "Level of verbosity printed to the output during the execution."
    }

    // prepare neo4jUrl
    this.updateNeo4jUrl()

  }

  ngAfterViewChecked() {
    this.enablePopovers();
  }

  enablePopovers() {
    (<any>$('[data-toggle="popover"]')).popover({
      title: '',
      placement: 'right',
      trigger: 'focus'
    });
  }

  getStep() {
    return this.step;
  }

  switchConfigStep(step) {
    this.step = step;
  }

  updateNeo4jUrl() {
    var regExp = new RegExp(/(<HOST>|<PORT>)/);
    this.urlSplit = this.urlPattern.split(regExp, 4);
    this.urlSplit[1] = this.dbConnection.host;
    this.urlSplit[3] = this.dbConnection.port;
    this.config.neo4jUrl = this.urlSplit.join("");
  }

  testConnection() {
    this.neo4jImorterService.testConnection(this.config).then((data) => {
      this.notification.push({content: "Connection is alive", autoHide: true});
    }).catch((error) => {
      this.notification.push({content: error.json(), error: true, autoHide: true});
    });
  }

  launch() {

    this.neo4jImorterService.launch(this.config).then((data) => {
      this.step = "running";
      this.jobRunning = true;
      this.status();
    }).catch(function (error) {
      console.log(error);
      alert("Error during migration!")
    });
  }

  status() {
    if(this.jobRunning) {
      this.neo4jImorterService.status().then((data) => {
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
    var logArea = $("#logArea");
    logArea.scrollTop(9999999);
  }

}

angular.module('neo4jimporter.components', []).directive(
  `neo4jimporter`,
  downgradeComponent({component: Neo4jImporterComponent}));


export {Neo4jImporterComponent};
