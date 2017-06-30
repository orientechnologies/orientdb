import {Component, NgZone, AfterViewChecked, EventEmitter, Output, ViewChild} from '@angular/core';

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
      "host": "localhost",
      "port": "5432",
      "dbName": "dvdrental",
      "sid": ""
    }

    this.jurlSplit = [];

    this.defaultConfig = {
      "driver": "PostgreSQL",
      "jurl": "",
      "username": "postgres",
      "password": "postgres",
      "protocol": "plocal",
      "outDBName": "testdb",
      "outDbUrl": "",
      "strategy": "naive",
      "mapper": "basicDBMapper",
      "xmlPath": "",
      "nameResolver": "original",
      "level": "2",
      "includedTables": []
    }

    this.config = angular.copy(this.defaultConfig);
    this.step = '0';

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
    this.teleporterService.testConnection(this.config).then((data) => {
      this.notification.push({content: "Connection is alive", autoHide: true});
    }).catch((error) => {
      this.notification.push({content: error.json(), error: true, autoHide: true});
    });
  }

  launch() {

    // fetching modelling config if it's not undefined and preparation
    if(this.modellingConfig) {

      // copying the configuration object
      var configCopy = JSON.parse(JSON.stringify(this.modellingConfig));

      // deleting source and target for each edge definition
      for (var edge of configCopy.edges) {
        delete edge.source;
        delete edge.target;
      }

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
      this.config.migrationConfig = JSON.stringify(configCopy);
    }

    this.config.outDbUrl = this.config.protocol + ":" + this.config.outDBName;

    // transforming includedTables if set
    if(this.includedTables.length > 0) {
      for(var i=0; i < this.includedTables.length; i++) {
        this.config.includedTables.push(this.includedTables[i].tableName);
      }
    }

    // invalidate the old migration config
    this.modellingConfig = undefined;
    this.selectedElement = undefined;
    this.graphPanel.invalidateMigrationConfig();

    this.teleporterService.launch(this.config).then((data) => {
      this.step = "running";
      this.jobRunning = true;
      this.status();
    }).catch(function (error) {
      alert("Error during migration!")
    });
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

  buildConfigJSON() {
    this.modellingConfig = {
      "vertices": [
        {
          "externalKey": ["address_id"],
          "mapping": {"sourceTables":[{"name":"PostgreSQL_address","dataSource":"PostgreSQL","tableName":"address","primaryKey":["address_id"]}]},
          "name": "address",
          "properties": {"address":{"include":true,"mapping":{"source":"PostgreSQL_address","type":"varchar","columnName":"address"},"notNull":false,"readOnly":false,"type":"STRING","ordinalPosition":2,"mandatory":false},"address2":{"include":true,"mapping":{"source":"PostgreSQL_address","type":"varchar","columnName":"address2"},"notNull":false,"readOnly":false,"type":"STRING","ordinalPosition":3,"mandatory":false},"phone":{"include":true,"mapping":{"source":"PostgreSQL_address","type":"varchar","columnName":"phone"},"notNull":false,"readOnly":false,"type":"STRING","ordinalPosition":7,"mandatory":false},"district":{"include":true,"mapping":{"source":"PostgreSQL_address","type":"varchar","columnName":"district"},"notNull":false,"readOnly":false,"type":"STRING","ordinalPosition":4,"mandatory":false},"last_update":{"include":true,"mapping":{"source":"PostgreSQL_address","type":"timestamp","columnName":"last_update"},"notNull":false,"readOnly":false,"type":"DATETIME","ordinalPosition":8,"mandatory":false},"address_id":{"include":true,"mapping":{"source":"PostgreSQL_address","type":"serial","columnName":"address_id"},"notNull":false,"readOnly":false,"type":"INTEGER","ordinalPosition":1,"mandatory":false},"postal_code":{"include":true,"mapping":{"source":"PostgreSQL_address","type":"varchar","columnName":"postal_code"},"notNull":false,"readOnly":false,"type":"STRING","ordinalPosition":6,"mandatory":false},"city_id":{"include":true,"mapping":{"source":"PostgreSQL_address","type":"int2","columnName":"city_id"},"notNull":false,"readOnly":false,"type":"SHORT","ordinalPosition":5,"mandatory":false}}
        },
        {
          "externalKey": ["country_id"],
          "mapping": {"sourceTables":[{"name":"PostgreSQL_country","dataSource":"PostgreSQL","tableName":"country","primaryKey":["country_id"]}]},
          "name": "country",
          "properties": {"country":{"include":true,"mapping":{"source":"PostgreSQL_country","type":"varchar","columnName":"country"},"notNull":false,"readOnly":false,"type":"STRING","ordinalPosition":2,"mandatory":false},"last_update":{"include":true,"mapping":{"source":"PostgreSQL_country","type":"timestamp","columnName":"last_update"},"notNull":false,"readOnly":false,"type":"DATETIME","ordinalPosition":3,"mandatory":false},"country_id":{"include":true,"mapping":{"source":"PostgreSQL_country","type":"serial","columnName":"country_id"},"notNull":false,"readOnly":false,"type":"INTEGER","ordinalPosition":1,"mandatory":false}}
        },
        {
          "externalKey": ["rental_id"],
          "mapping": {"sourceTables":[{"name":"PostgreSQL_rental","dataSource":"PostgreSQL","tableName":"rental","primaryKey":["rental_id"]}]},
          "name": "rental",
          "properties": {"inventory_id":{"include":true,"mapping":{"source":"PostgreSQL_rental","type":"int4","columnName":"inventory_id"},"notNull":false,"readOnly":false,"type":"INTEGER","ordinalPosition":3,"mandatory":false},"staff_id":{"include":true,"mapping":{"source":"PostgreSQL_rental","type":"int2","columnName":"staff_id"},"notNull":false,"readOnly":false,"type":"SHORT","ordinalPosition":6,"mandatory":false},"last_update":{"include":true,"mapping":{"source":"PostgreSQL_rental","type":"timestamp","columnName":"last_update"},"notNull":false,"readOnly":false,"type":"DATETIME","ordinalPosition":7,"mandatory":false},"rental_date":{"include":true,"mapping":{"source":"PostgreSQL_rental","type":"timestamp","columnName":"rental_date"},"notNull":false,"readOnly":false,"type":"DATETIME","ordinalPosition":2,"mandatory":false},"customer_id":{"include":true,"mapping":{"source":"PostgreSQL_rental","type":"int2","columnName":"customer_id"},"notNull":false,"readOnly":false,"type":"SHORT","ordinalPosition":4,"mandatory":false},"rental_id":{"include":true,"mapping":{"source":"PostgreSQL_rental","type":"serial","columnName":"rental_id"},"notNull":false,"readOnly":false,"type":"INTEGER","ordinalPosition":1,"mandatory":false},"return_date":{"include":true,"mapping":{"source":"PostgreSQL_rental","type":"timestamp","columnName":"return_date"},"notNull":false,"readOnly":false,"type":"DATETIME","ordinalPosition":5,"mandatory":false}}
        },
        {
          "externalKey": ["store_id"],
          "mapping": {"sourceTables":[{"name":"PostgreSQL_store","dataSource":"PostgreSQL","tableName":"store","primaryKey":["store_id"]}]},
          "name": "store",
          "properties": {"store_id":{"include":true,"mapping":{"source":"PostgreSQL_store","type":"serial","columnName":"store_id"},"notNull":false,"readOnly":false,"type":"INTEGER","ordinalPosition":1,"mandatory":false},"manager_staff_id":{"include":true,"mapping":{"source":"PostgreSQL_store","type":"int2","columnName":"manager_staff_id"},"notNull":false,"readOnly":false,"type":"SHORT","ordinalPosition":2,"mandatory":false},"last_update":{"include":true,"mapping":{"source":"PostgreSQL_store","type":"timestamp","columnName":"last_update"},"notNull":false,"readOnly":false,"type":"DATETIME","ordinalPosition":4,"mandatory":false},"address_id":{"include":true,"mapping":{"source":"PostgreSQL_store","type":"int2","columnName":"address_id"},"notNull":false,"readOnly":false,"type":"SHORT","ordinalPosition":3,"mandatory":false}}
        },
        {
          "externalKey": ["film_id"],
          "mapping": {"sourceTables":[{"name":"PostgreSQL_film","dataSource":"PostgreSQL","tableName":"film","primaryKey":["film_id"]}]},
          "name": "film",
          "properties": {"special_features":{"include":true,"mapping":{"source":"PostgreSQL_film","type":"_text","columnName":"special_features"},"notNull":false,"readOnly":false,"type":"STRING","ordinalPosition":12,"mandatory":false},"rental_duration":{"include":true,"mapping":{"source":"PostgreSQL_film","type":"int2","columnName":"rental_duration"},"notNull":false,"readOnly":false,"type":"SHORT","ordinalPosition":6,"mandatory":false},"rental_rate":{"include":true,"mapping":{"source":"PostgreSQL_film","type":"numeric","columnName":"rental_rate"},"notNull":false,"readOnly":false,"type":"DECIMAL","ordinalPosition":7,"mandatory":false},"release_year":{"include":true,"mapping":{"source":"PostgreSQL_film","type":"year","columnName":"release_year"},"notNull":false,"readOnly":false,"type":"STRING","ordinalPosition":4,"mandatory":false},"length":{"include":true,"mapping":{"source":"PostgreSQL_film","type":"int2","columnName":"length"},"notNull":false,"readOnly":false,"type":"SHORT","ordinalPosition":8,"mandatory":false},"replacement_cost":{"include":true,"mapping":{"source":"PostgreSQL_film","type":"numeric","columnName":"replacement_cost"},"notNull":false,"readOnly":false,"type":"DECIMAL","ordinalPosition":9,"mandatory":false},"rating":{"include":true,"mapping":{"source":"PostgreSQL_film","type":"mpaa_rating","columnName":"rating"},"notNull":false,"readOnly":false,"type":"STRING","ordinalPosition":10,"mandatory":false},"description":{"include":true,"mapping":{"source":"PostgreSQL_film","type":"text","columnName":"description"},"notNull":false,"readOnly":false,"type":"STRING","ordinalPosition":3,"mandatory":false},"language_id":{"include":true,"mapping":{"source":"PostgreSQL_film","type":"int2","columnName":"language_id"},"notNull":false,"readOnly":false,"type":"SHORT","ordinalPosition":5,"mandatory":false},"title":{"include":true,"mapping":{"source":"PostgreSQL_film","type":"varchar","columnName":"title"},"notNull":false,"readOnly":false,"type":"STRING","ordinalPosition":2,"mandatory":false},"last_update":{"include":true,"mapping":{"source":"PostgreSQL_film","type":"timestamp","columnName":"last_update"},"notNull":false,"readOnly":false,"type":"DATETIME","ordinalPosition":11,"mandatory":false},"fulltext":{"include":true,"mapping":{"source":"PostgreSQL_film","type":"tsvector","columnName":"fulltext"},"notNull":false,"readOnly":false,"type":"STRING","ordinalPosition":13,"mandatory":false},"film_id":{"include":true,"mapping":{"source":"PostgreSQL_film","type":"serial","columnName":"film_id"},"notNull":false,"readOnly":false,"type":"INTEGER","ordinalPosition":1,"mandatory":false}}
        },
        {
          "externalKey": ["inventory_id"],
          "mapping": {"sourceTables":[{"name":"PostgreSQL_inventory","dataSource":"PostgreSQL","tableName":"inventory","primaryKey":["inventory_id"]}]},
          "name": "inventory",
          "properties": {"store_id":{"include":true,"mapping":{"source":"PostgreSQL_inventory","type":"int2","columnName":"store_id"},"notNull":false,"readOnly":false,"type":"SHORT","ordinalPosition":3,"mandatory":false},"inventory_id":{"include":true,"mapping":{"source":"PostgreSQL_inventory","type":"serial","columnName":"inventory_id"},"notNull":false,"readOnly":false,"type":"INTEGER","ordinalPosition":1,"mandatory":false},"last_update":{"include":true,"mapping":{"source":"PostgreSQL_inventory","type":"timestamp","columnName":"last_update"},"notNull":false,"readOnly":false,"type":"DATETIME","ordinalPosition":4,"mandatory":false},"film_id":{"include":true,"mapping":{"source":"PostgreSQL_inventory","type":"int2","columnName":"film_id"},"notNull":false,"readOnly":false,"type":"SHORT","ordinalPosition":2,"mandatory":false}}
        },
        {
          "externalKey": ["customer_id"],
          "mapping": {"sourceTables":[{"name":"PostgreSQL_customer","dataSource":"PostgreSQL","tableName":"customer","primaryKey":["customer_id"]}]},
          "name": "customer",
          "properties": {"store_id":{"include":true,"mapping":{"source":"PostgreSQL_customer","type":"int2","columnName":"store_id"},"notNull":false,"readOnly":false,"type":"SHORT","ordinalPosition":2,"mandatory":false},"last_update":{"include":true,"mapping":{"source":"PostgreSQL_customer","type":"timestamp","columnName":"last_update"},"notNull":false,"readOnly":false,"type":"DATETIME","ordinalPosition":9,"mandatory":false},"address_id":{"include":true,"mapping":{"source":"PostgreSQL_customer","type":"int2","columnName":"address_id"},"notNull":false,"readOnly":false,"type":"SHORT","ordinalPosition":6,"mandatory":false},"last_name":{"include":true,"mapping":{"source":"PostgreSQL_customer","type":"varchar","columnName":"last_name"},"notNull":false,"readOnly":false,"type":"STRING","ordinalPosition":4,"mandatory":false},"active":{"include":true,"mapping":{"source":"PostgreSQL_customer","type":"int4","columnName":"active"},"notNull":false,"readOnly":false,"type":"INTEGER","ordinalPosition":10,"mandatory":false},"activebool":{"include":true,"mapping":{"source":"PostgreSQL_customer","type":"bool","columnName":"activebool"},"notNull":false,"readOnly":false,"type":"BOOLEAN","ordinalPosition":7,"mandatory":false},"customer_id":{"include":true,"mapping":{"source":"PostgreSQL_customer","type":"serial","columnName":"customer_id"},"notNull":false,"readOnly":false,"type":"INTEGER","ordinalPosition":1,"mandatory":false},"create_date":{"include":true,"mapping":{"source":"PostgreSQL_customer","type":"date","columnName":"create_date"},"notNull":false,"readOnly":false,"type":"DATE","ordinalPosition":8,"mandatory":false},"first_name":{"include":true,"mapping":{"source":"PostgreSQL_customer","type":"varchar","columnName":"first_name"},"notNull":false,"readOnly":false,"type":"STRING","ordinalPosition":3,"mandatory":false},"email":{"include":true,"mapping":{"source":"PostgreSQL_customer","type":"varchar","columnName":"email"},"notNull":false,"readOnly":false,"type":"STRING","ordinalPosition":5,"mandatory":false}}
        },
        {
          "externalKey": ["payment_id"],
          "mapping": {"sourceTables":[{"name":"PostgreSQL_payment","dataSource":"PostgreSQL","tableName":"payment","primaryKey":["payment_id"]}]},
          "name": "payment",
          "properties": {"amount":{"include":true,"mapping":{"source":"PostgreSQL_payment","type":"numeric","columnName":"amount"},"notNull":false,"readOnly":false,"type":"DECIMAL","ordinalPosition":5,"mandatory":false},"payment_id":{"include":true,"mapping":{"source":"PostgreSQL_payment","type":"serial","columnName":"payment_id"},"notNull":false,"readOnly":false,"type":"INTEGER","ordinalPosition":1,"mandatory":false},"staff_id":{"include":true,"mapping":{"source":"PostgreSQL_payment","type":"int2","columnName":"staff_id"},"notNull":false,"readOnly":false,"type":"SHORT","ordinalPosition":3,"mandatory":false},"customer_id":{"include":true,"mapping":{"source":"PostgreSQL_payment","type":"int2","columnName":"customer_id"},"notNull":false,"readOnly":false,"type":"SHORT","ordinalPosition":2,"mandatory":false},"payment_date":{"include":true,"mapping":{"source":"PostgreSQL_payment","type":"timestamp","columnName":"payment_date"},"notNull":false,"readOnly":false,"type":"DATETIME","ordinalPosition":6,"mandatory":false},"rental_id":{"include":true,"mapping":{"source":"PostgreSQL_payment","type":"int4","columnName":"rental_id"},"notNull":false,"readOnly":false,"type":"INTEGER","ordinalPosition":4,"mandatory":false}}
        },
        {
          "externalKey": ["actor_id"],
          "mapping": {"sourceTables":[{"name":"PostgreSQL_actor","dataSource":"PostgreSQL","tableName":"actor","primaryKey":["actor_id"]}]},
          "name": "actor",
          "properties": {"last_update":{"include":true,"mapping":{"source":"PostgreSQL_actor","type":"timestamp","columnName":"last_update"},"notNull":false,"readOnly":false,"type":"DATETIME","ordinalPosition":4,"mandatory":false},"last_name":{"include":true,"mapping":{"source":"PostgreSQL_actor","type":"varchar","columnName":"last_name"},"notNull":false,"readOnly":false,"type":"STRING","ordinalPosition":3,"mandatory":false},"actor_id":{"include":true,"mapping":{"source":"PostgreSQL_actor","type":"serial","columnName":"actor_id"},"notNull":false,"readOnly":false,"type":"INTEGER","ordinalPosition":1,"mandatory":false},"first_name":{"include":true,"mapping":{"source":"PostgreSQL_actor","type":"varchar","columnName":"first_name"},"notNull":false,"readOnly":false,"type":"STRING","ordinalPosition":2,"mandatory":false}}
        },
        {
          "externalKey": ["city_id"],
          "mapping": {"sourceTables":[{"name":"PostgreSQL_city","dataSource":"PostgreSQL","tableName":"city","primaryKey":["city_id"]}]},
          "name": "city",
          "properties": {"city":{"include":true,"mapping":{"source":"PostgreSQL_city","type":"varchar","columnName":"city"},"notNull":false,"readOnly":false,"type":"STRING","ordinalPosition":2,"mandatory":false},"last_update":{"include":true,"mapping":{"source":"PostgreSQL_city","type":"timestamp","columnName":"last_update"},"notNull":false,"readOnly":false,"type":"DATETIME","ordinalPosition":4,"mandatory":false},"country_id":{"include":true,"mapping":{"source":"PostgreSQL_city","type":"int2","columnName":"country_id"},"notNull":false,"readOnly":false,"type":"SHORT","ordinalPosition":3,"mandatory":false},"city_id":{"include":true,"mapping":{"source":"PostgreSQL_city","type":"serial","columnName":"city_id"},"notNull":false,"readOnly":false,"type":"INTEGER","ordinalPosition":1,"mandatory":false}}
        },
        {
          "externalKey": ["language_id"],
          "mapping": {"sourceTables":[{"name":"PostgreSQL_language","dataSource":"PostgreSQL","tableName":"language","primaryKey":["language_id"]}]},
          "name": "language",
          "properties": {"last_update":{"include":true,"mapping":{"source":"PostgreSQL_language","type":"timestamp","columnName":"last_update"},"notNull":false,"readOnly":false,"type":"DATETIME","ordinalPosition":3,"mandatory":false},"name":{"include":true,"mapping":{"source":"PostgreSQL_language","type":"bpchar","columnName":"name"},"notNull":false,"readOnly":false,"type":"STRING","ordinalPosition":2,"mandatory":false},"language_id":{"include":true,"mapping":{"source":"PostgreSQL_language","type":"serial","columnName":"language_id"},"notNull":false,"readOnly":false,"type":"INTEGER","ordinalPosition":1,"mandatory":false}}
        },
        {
          "externalKey": ["staff_id"],
          "mapping": {"sourceTables":[{"name":"PostgreSQL_staff","dataSource":"PostgreSQL","tableName":"staff","primaryKey":["staff_id"]}]},
          "name": "staff",
          "properties": {"store_id":{"include":true,"mapping":{"source":"PostgreSQL_staff","type":"int2","columnName":"store_id"},"notNull":false,"readOnly":false,"type":"SHORT","ordinalPosition":6,"mandatory":false},"password":{"include":true,"mapping":{"source":"PostgreSQL_staff","type":"varchar","columnName":"password"},"notNull":false,"readOnly":false,"type":"STRING","ordinalPosition":9,"mandatory":false},"staff_id":{"include":true,"mapping":{"source":"PostgreSQL_staff","type":"serial","columnName":"staff_id"},"notNull":false,"readOnly":false,"type":"INTEGER","ordinalPosition":1,"mandatory":false},"last_update":{"include":true,"mapping":{"source":"PostgreSQL_staff","type":"timestamp","columnName":"last_update"},"notNull":false,"readOnly":false,"type":"DATETIME","ordinalPosition":10,"mandatory":false},"address_id":{"include":true,"mapping":{"source":"PostgreSQL_staff","type":"int2","columnName":"address_id"},"notNull":false,"readOnly":false,"type":"SHORT","ordinalPosition":4,"mandatory":false},"last_name":{"include":true,"mapping":{"source":"PostgreSQL_staff","type":"varchar","columnName":"last_name"},"notNull":false,"readOnly":false,"type":"STRING","ordinalPosition":3,"mandatory":false},"active":{"include":true,"mapping":{"source":"PostgreSQL_staff","type":"bool","columnName":"active"},"notNull":false,"readOnly":false,"type":"BOOLEAN","ordinalPosition":7,"mandatory":false},"first_name":{"include":true,"mapping":{"source":"PostgreSQL_staff","type":"varchar","columnName":"first_name"},"notNull":false,"readOnly":false,"type":"STRING","ordinalPosition":2,"mandatory":false},"email":{"include":true,"mapping":{"source":"PostgreSQL_staff","type":"varchar","columnName":"email"},"notNull":false,"readOnly":false,"type":"STRING","ordinalPosition":5,"mandatory":false},"picture":{"include":true,"mapping":{"source":"PostgreSQL_staff","type":"bytea","columnName":"picture"},"notNull":false,"readOnly":false,"type":"BINARY","ordinalPosition":11,"mandatory":false},"username":{"include":true,"mapping":{"source":"PostgreSQL_staff","type":"varchar","columnName":"username"},"notNull":false,"readOnly":false,"type":"STRING","ordinalPosition":8,"mandatory":false}}
        },
        {
          "externalKey": ["category_id"],
          "mapping": {"sourceTables":[{"name":"PostgreSQL_category","dataSource":"PostgreSQL","tableName":"category","primaryKey":["category_id"]}]},
          "name": "category",
          "properties": {"category_id":{"include":true,"mapping":{"source":"PostgreSQL_category","type":"serial","columnName":"category_id"},"notNull":false,"readOnly":false,"type":"INTEGER","ordinalPosition":1,"mandatory":false},"last_update":{"include":true,"mapping":{"source":"PostgreSQL_category","type":"timestamp","columnName":"last_update"},"notNull":false,"readOnly":false,"type":"DATETIME","ordinalPosition":3,"mandatory":false},"name":{"include":true,"mapping":{"source":"PostgreSQL_category","type":"varchar","columnName":"name"},"notNull":false,"readOnly":false,"type":"STRING","ordinalPosition":2,"mandatory":false}}
        },
        {
          "externalKey": ["subtitle_id"],
          "mapping": {"sourceTables":[{"name":"PostgreSQL_subtitle","dataSource":"PostgreSQL","tableName":"subtitle","primaryKey":["subtitle_id"]}]},
          "name": "subtitle",
          "properties": {"name":{"include":true,"mapping":{"source":"PostgreSQL_subtitle","type":"bpchar","columnName":"name"},"notNull":false,"readOnly":false,"type":"STRING","ordinalPosition":2,"mandatory":false},"subtitle_id":{"include":true,"mapping":{"source":"PostgreSQL_subtitle","type":"serial","columnName":"subtitle_id"},"notNull":false,"readOnly":false,"type":"INTEGER","ordinalPosition":1,"mandatory":false}}
        }
      ],
      "edges": [
        {
          "has_city": {"mapping":[{"fromColumns":["city_id"],"toTable":"city","toColumns":["city_id"],"fromTable":"address","direction":"direct"}],"isLogical":false,"properties":{}}
        },
        {
          "has_country": {"mapping":[{"fromColumns":["country_id"],"toTable":"country","toColumns":["country_id"],"fromTable":"city","direction":"direct"}],"isLogical":false,"properties":{}}
        },
        {
          "has_address": {"mapping":[{"fromColumns":["address_id"],"toTable":"address","toColumns":["address_id"],"fromTable":"store","direction":"direct"},{"fromColumns":["address_id"],"toTable":"address","toColumns":["address_id"],"fromTable":"store","direction":"direct"},{"fromColumns":["address_id"],"toTable":"address","toColumns":["address_id"],"fromTable":"store","direction":"direct"}],"isLogical":false,"properties":{}}
        },
        {
          "has_language": {"mapping":[{"fromColumns":["language_id"],"toTable":"language","toColumns":["language_id"],"fromTable":"film","direction":"direct"}],"isLogical":false,"properties":{}}
        },
        {
          "has_film": {"mapping":[{"fromColumns":["film_id"],"toTable":"film","toColumns":["film_id"],"fromTable":"inventory","direction":"direct"},{"fromColumns":["film_id"],"toTable":"film","toColumns":["film_id"],"fromTable":"inventory","direction":"direct"},{"fromColumns":["film_id"],"toTable":"film","toColumns":["film_id"],"fromTable":"inventory","direction":"direct"}],"isLogical":false,"properties":{}}
        },
        {
          "has_customer": {"mapping":[{"fromColumns":["customer_id"],"toTable":"customer","toColumns":["customer_id"],"fromTable":"rental","direction":"direct"},{"fromColumns":["customer_id"],"toTable":"customer","toColumns":["customer_id"],"fromTable":"rental","direction":"direct"}],"isLogical":false,"properties":{}}
        },
        {
          "has_rental": {"mapping":[{"fromColumns":["rental_id"],"toTable":"rental","toColumns":["rental_id"],"fromTable":"payment","direction":"direct"}],"isLogical":false,"properties":{}}
        },
        {
          "has_staff": {"mapping":[{"fromColumns":["staff_id"],"toTable":"staff","toColumns":["staff_id"],"fromTable":"rental","direction":"direct"},{"fromColumns":["staff_id"],"toTable":"staff","toColumns":["staff_id"],"fromTable":"rental","direction":"direct"}],"isLogical":false,"properties":{}}
        },
        {
          "has_inventory": {"mapping":[{"fromColumns":["inventory_id"],"toTable":"inventory","toColumns":["inventory_id"],"fromTable":"rental","direction":"direct"}],"isLogical":false,"properties":{}}
        },
        {
          "has_manager_staff": {"mapping":[{"fromColumns":["manager_staff_id"],"toTable":"staff","toColumns":["staff_id"],"fromTable":"store","direction":"direct"}],"isLogical":false,"properties":{}}
        },
        {
          "film_actor": {"mapping":[{"fromColumns":["actor_id"],"toTable":"film","toColumns":["film_id"],"joinTable":{"fromColumns":["actor_id"],"toColumns":["film_id"],"tableName":"film_actor"},"fromTable":"actor","direction":"direct"}],"isLogical":false,"properties":{"last_update":{"include":true,"mapping":{"source":"PostgreSQL_film_actor","type":"timestamp","columnName":"last_update"},"notNull":false,"readOnly":false,"type":"DATETIME","ordinalPosition":1,"mandatory":false}}}
        },
        {
          "film_category": {"mapping":[{"fromColumns":["category_id"],"toTable":"film","toColumns":["film_id"],"joinTable":{"fromColumns":["category_id"],"toColumns":["film_id"],"tableName":"film_category"},"fromTable":"category","direction":"direct"}],"isLogical":false,"properties":{"last_update":{"include":true,"mapping":{"source":"PostgreSQL_film_category","type":"timestamp","columnName":"last_update"},"notNull":false,"readOnly":false,"type":"DATETIME","ordinalPosition":1,"mandatory":false}}}
        }
      ]
    }
  }

}

angular.module('teleporter.components', []).directive(
  `teleporter`,
  downgradeComponent({component: TeleporterComponent}));


export {TeleporterComponent};
