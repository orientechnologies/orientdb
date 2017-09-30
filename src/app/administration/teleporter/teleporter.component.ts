import {Component, NgZone, AfterViewChecked, EventEmitter, Output, ViewChild} from '@angular/core';
import {ModalComponent} from 'ng2-bs3-modal/ng2-bs3-modal';

import * as $ from "jquery"

import {downgradeComponent} from '@angular/upgrade/static';
import {TeleporterService} from '../../core/services';
import {NotificationService} from "../../core/services/notification.service";

declare var angular:any

@Component({
  selector: 'teleporter',
  templateUrl: "./teleporter.component.html",
  styleUrls: []
})

class TeleporterComponent implements AfterViewChecked {

  private popoversEnabled:boolean;

  private protocols;
  private strategies;
  private nameResolvers;
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

  // dual list box
  private keepSorted;
  private key;
  private fieldToDisplay;
  private sourceDBTables = [];
  private includedTables = [];

  // JSON configuration for modelling
  private modellingConfig;
  private selectedElement;
  private configFetched:boolean;

  // Event Emitter
  @Output() onMigrationConfigFetched = new EventEmitter();

  @ViewChild('detailPanel') detailPanel;
  @ViewChild('graphPanel') graphPanel;

  // Modals' variables
  @ViewChild('acceptOracleLicenseModal') acceptOracleLicenseModal: ModalComponent;
  private licensePermission = false;


  constructor(private teleporterService: TeleporterService, private notification: NotificationService,
              private zone: NgZone) {

    this.init();
  }

  init() {

    this.popoversEnabled = false;

    this.protocols = ["plocal", "memory"];
    this.strategies = ["naive", "naive-aggregate"];
    this.nameResolvers = ["original", "java"];
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
      "outDBName": "",
      "outDbUrl": "",
      "strategy": "naive",
      "mapper": "basicDBMapper",
      "xmlPath": "",
      "nameResolver": "original",
      "level": "2",
      "includedTables": []
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

    // initialising job info
    this.initJobInfo();
    this.jobRunning = false;

    this.hints = {
      driver: "Driver name of the DBMS from which you want to execute the migration.",
      host: "Address of the host where the DBMS is available.",
      port: "The port where your DBMS is listening for new connections.",
      dbName: "The source database name.",
      sid: "SID is a unique name for an Oracle database instance.",
      username: "The username to access the source database.",
      password: "The password to access the source database.",
      protocol: "The protocol to use during the migration in order to connect to OrientDB:<br>" +
      "<li><b>plocal</b>: the dabase will run locally in the same JVM of your application.</li>" +
      "<li><b>remote</b>: the database will be accessed via TCP/IP connection.</li>",
      outDBName: "The name for the destination OrientDB graph database.",
      strategy: "Strategy adopted during the importing phase.<br> " +
      "<li><b>naive</b>: performs a 'naive' import of the data source. The data source schema is translated semi-directly in a correspondent and coherent graph model.</li> " +
      "<li><b>naive-aggregate</b>: performs a 'naive' import of the data source. The data source schema is translated semi-directly in a correspondent and coherent graph model " +
      "using an aggregation policy on the junction tables of dimension equals to 2.</li><br>" +
      "<a href='http://orientdb.com/docs/last/Teleporter-Execution-Strategies.html'>More info</a>",
      nameResolver: "Name of the resolver which transforms the names of all the elements of the source database according to a specific convention.<br>" +
      "<li><b>original</b>: maintains the original name convention.</li>" +
      "<li><b>java</b>: transforms all the elements' names according to the Java convention.</li>",
      tableList: "Select the source database tables you want to import in OrientDB.",
      XMLPath: "Executes the migration taking advantage of OrientDB's polymorphism according to the configuration in the specified XML file.<br><br>" +
      "<a href='http://orientdb.com/docs/last/Teleporter-Inheritance.html'>More info</a>",
      logLevel: "Level of verbosity printed to the output during the execution."
    }

    // dual list box
    this.keepSorted = true;
    this.key = "id";
    this.fieldToDisplay = "tableName";

    // this.buildConfigJSON();

    this.selectedElement = undefined;
    this.configFetched = false;
  }

  ngAfterViewChecked() {
    if(!this.popoversEnabled) {
      this.enablePopovers();
      this.popoversEnabled = true;
    }
  }

  enablePopovers() {
    (<any>$('[data-toggle="popover"]')).popover({
      title: '',
      placement: 'right',
      trigger: 'focus'
    });
  }

  initJobInfo() {
    this.job = {cfg: undefined, status: undefined, log: ""};
  }

  getStep() {
    return this.step;
  }

  switchConfigStep(step) {

    if(step === '3') {

      // fetching tables' names
      this.getTablesNames().then((data) => {
        this.sourceDBTables = data["tables"];
      })
    }
    else if(step === '4') {

      // fetching migration config
      this.getMigrationConfig();
    }
    this.step = step;

    // popovers need being enabled
    this.popoversEnabled = false;
  }

  changeSelectedElement(e) {
    this.selectedElement = e;
    this.detailPanel.changeSelectedElement(e);
  }

  drivers() {
    return this.teleporterService.drivers();
  }

  getTablesNames() {
    return this.teleporterService.getTablesNames(this.config);
  }

  getMigrationConfig() {
    this.teleporterService.getMigrationConfig(this.config).then((data) => {
      this.modellingConfig = data;
      this.onMigrationConfigFetched.emit();
    }).catch((error) => {
      this.notification.push({content: error.json(), error: true, autoHide: true});
    });
  }

  updateIncludedTables(includedTables) {

    var includedTablesRaw = [];

    for(var i=0; i<includedTables.length; i++) {
      includedTablesRaw[i] = includedTables[i].tableName;
    }

    this.config.includedTables = includedTablesRaw;
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
    this.teleporterService.testConnection(this.config).then(() => {
      this.notification.push({content: "Connection is alive", autoHide: true});
    }).catch((error) => {
      this.notification.push({content: error.json(), error: true, autoHide: true});
    });
  }

  saveConfiguration() {

    var migrationConfigString;

    // fetching modelling config if it's not undefined and preparation
    if(this.modellingConfig) {

      // copying the configuration object
      var configCopy = JSON.parse(JSON.stringify(this.modellingConfig));

      this.prepareModellingConfig(configCopy);
      migrationConfigString = JSON.stringify(configCopy);
    }

    var params = {
      migrationConfig: migrationConfigString,
      outDBName: this.config.outDBName
    };
    this.teleporterService.saveConfiguration(params).then(() => {
      this.notification.push({content: "Configuration correctly saved.", autoHide: true});
    }).catch((error) => {
      this.notification.push({content: error.json(), error: true, autoHide: true});
    });
  }

  launch() {

    if(this.config.driver === 'Oracle' && this.licensePermission === false) {
      this.prepareAndOpenAcceptOracleLicenseModal();
    }
    else {

      // fetching modelling config if it's not undefined and preparation
      if (this.modellingConfig) {

        // copying the configuration object
        var configCopy = JSON.parse(JSON.stringify(this.modellingConfig));

        this.prepareModellingConfig(configCopy);
        this.config.migrationConfig = JSON.stringify(configCopy);
      }

      this.config.outDbUrl = this.config.protocol + ":" + this.config.outDBName;

      // transforming includedTables if set
      if (this.includedTables.length > 0) {
        for (var i = 0; i < this.includedTables.length; i++) {
          this.config.includedTables.push(this.includedTables[i].tableName);
        }
      }

      // invalidate the old migration config
      this.modellingConfig = undefined;
      this.selectedElement = undefined;
      if (this.graphPanel !== undefined) {
        this.graphPanel.invalidateMigrationConfig();
      }

      this.initJobInfo();

      this.step = "running";
      this.jobRunning = true;
      this.teleporterService.launch(this.config).then(() => {
        this.status();
      }).catch(function (error) {
        alert("Error during migration!")
      });
    }
  }

  prepareModellingConfig(configCopy) {

    // deleting source and target for each edge definition
    for (var edge of configCopy.edges) {
      delete edge.source;
      delete edge.target;
    }

    this.aggregateEdgesByNameInConfig(configCopy);
  }

  aggregateEdgesByNameInConfig(configCopy) {

    // aggregating edges with the same name (that is all the edges representing the same edge class)
    for (var i = 0; i < configCopy.edges.length; i++) {
      var elementToAggregate = configCopy.edges[i];
      var elementToAggregateName = this.getEdgeClassName(elementToAggregate);
      for (var j = i + 1; j < configCopy.edges.length; j++) {
        var currEdge = configCopy.edges[j];
        var currEdgeName = this.getEdgeClassName(currEdge);

        if (elementToAggregateName === currEdgeName) {

          // aggregate mapping information
          elementToAggregate[elementToAggregateName].mapping.push(currEdge[currEdgeName].mapping[0]);

          // delete the duplicate edges
          configCopy.edges.splice(j, 1);

          // decreasing j as the array shifted after the delete
          j--;
        }
      }
    }
  }

  getEdgeClassName(link) {

    var edgeClassName = undefined;
    var keys = Object.keys(link);

    for(var key of keys) {
      if(key !== 'source' && key !== 'target') {
        edgeClassName = key;
        break;
      }
    }

    return edgeClassName;
  }

  status() {
    if(this.jobRunning) {
      this.teleporterService.status().then((data) => {
        if(data.jobs.length > 0) {
          var currentJobInfo = data.jobs[0];
          this.job.cfg = currentJobInfo.cfg;
          this.job.status = currentJobInfo.status;
          this.job.log += currentJobInfo.log;

          this.scrollLogAreaDown();
        }
        else {
          if(this.job) {
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
        }, 3000);

      });
    }
  }

  scrollLogAreaDown() {
    var logArea = $("#logArea");
    logArea.scrollTop(9999999);
  }

  /**
   *
   * @param $event
   */
  renameElementInGraph(event) {
    this.graphPanel.renameElementInGraph(event);
  }

  /**
   *
   * @param $event
   */
  removeElementInGraph(event) {
    this.graphPanel.removeElementInGraph(event);
  }

  /**
   * ----------------------------------------------------------
   *
   *                      Modals' methods
   *
   * ----------------------------------------------------------
   */

  /**
   * It prepares and opens an "accept oracle license" modal.
   */
  prepareAndOpenAcceptOracleLicenseModal() {

    // opening the modal
    this.acceptOracleLicenseModal.open();
  }

  /**
   * It's called when an "accept oracle license" modal closing or dismissing occurs.
   */
  dismissOrCloseAcceptOracleLicenseModal(action) {

    if(action === 'accept') {

      // setting acceptOracleLicense true
      this.licensePermission = true;

      this.launch();

      // closing the modal
      this.acceptOracleLicenseModal.close();
    }
    else if(action === 'reject') {

      // setting permission to false
      this.licensePermission = false;

      // closing the modal
      this.acceptOracleLicenseModal.close();
    }
    else if(action === 'dismiss') {

      // setting permission to false
      this.licensePermission = false;

      // dismissing the modal
      this.acceptOracleLicenseModal.dismiss();
    }
  }

}

angular.module('teleporter.components', []).directive(
  `teleporter`,
  downgradeComponent({component: TeleporterComponent}));


export {TeleporterComponent};
