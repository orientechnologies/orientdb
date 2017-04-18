import {Component, NgZone, AfterViewChecked} from '@angular/core';

import * as $ from "jquery"

import {downgradeComponent} from '@angular/upgrade/static';
import {TeleporterService} from '../../core/services';
import {NotificationService} from "../../core/services/notification.service";
import {AgentService} from "../../core/services/agent.service";

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

  constructor(private teleporterService: TeleporterService, private notification: NotificationService,
              private agentService: AgentService, private zone: NgZone) {

    // agent
    this.agentService.isActive().then(() => {
      this.init();
    });

  }

  updateModellingConfig() {
    this.modellingConfig.vertices[0].name = "Parent";
  }

  init() {
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
      "outDBName": "dvdrental",
      "outDbUrl": "",
      "strategy": "naive",
      "mapper": "basicDBMapper",
      "xmlPath": "",
      "nameResolver": "original",
      "level": "2",
      "includedTables": [],
    }

    this.config = angular.copy(this.defaultConfig);
    this.step = '8';

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
      tableList: "Select the source database tables you want to import in OrientDB. If no tables are present try to connect with your source database again.",
      XMLPath: "Executes the migration taking advantage of OrientDB's polymorphism according to the configuration in the specified XML file.<br><br>" +
      "<a href='http://orientdb.com/docs/last/Teleporter-Inheritance.html'>More info</a>",
      logLevel: "Level of verbosity printed to the output during the execution."
    }

    // dual list box
    this.keepSorted = true;
    this.key = "id";
    this.fieldToDisplay = "tableName";

    this.buildConfigJSON();
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

    if(step === '5') {

      // fetching tables' names
      this.getTablesNames().then((data) => {
        this.sourceDBTables = data["tables"];
      })
    }
    this.step = step;
  }

  drivers() {
    return this.teleporterService.drivers();
  }

  getTablesNames() {
    return this.teleporterService.getTablesNames(this.config);
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

    this.config.outDbUrl = this.config.protocol + ":" + this.config.outDBName;

    // transforming includedTables if set
    if(this.includedTables.length > 0) {
      for(var i=0; i < this.includedTables.length; i++) {
        this.config.includedTables.push(this.includedTables[i].tableName);
      }
    }

    this.teleporterService.launch(this.config).then((data) => {
      this.step = "running";
      this.jobRunning = true;
      this.status();
    }).catch(function (error) {
      alert("Error during migration!")
    });
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

  buildConfigJSON() {
    this.modellingConfig = {
      "vertices": [
        {
          "name": "Project",
          "mapping": {
            "sourceTables": [
              {
                "name": "hsqldb_PROJECT",
                "dataSource": "hsqldb",
                "tableName": "PROJECT",
                "primaryKey": [
                  "ID"
                ]
              }
            ]
          },
          "externalKey": [
            "id"
          ],
          "properties": {
            "id": {
              "include": true,
              "type": "STRING",
              "ordinalPosition": 1,
              "mandatory": false,
              "readOnly": false,
              "notNull": false,
              "mapping": {
                "source": "hsqldb_PROJECT",
                "columnName": "ID",
                "type": "VARCHAR"
              }
            },
            "projectName": {
              "include": true,
              "type": "STRING",
              "ordinalPosition": 2,
              "mandatory": false,
              "readOnly": false,
              "notNull": false,
              "mapping": {
                "source": "hsqldb_PROJECT",
                "columnName": "PROJECT_NAME",
                "type": "VARCHAR"
              }
            },
            "description": {
              "include": true,
              "type": "STRING",
              "ordinalPosition": 3,
              "mandatory": false,
              "readOnly": false,
              "notNull": false,
              "mapping": {
                "source": "hsqldb_PROJECT",
                "columnName": "DESCRIPTION",
                "type": "VARCHAR"
              }
            },
            "startDate": {
              "include": true,
              "type": "DATE",
              "ordinalPosition": 4,
              "mandatory": false,
              "readOnly": false,
              "notNull": false,
              "mapping": {
                "source": "hsqldb_PROJECT",
                "columnName": "START_DATE",
                "type": "DATE"
              }
            },
            "expectedEndDate": {
              "include": true,
              "type": "DATE",
              "ordinalPosition": 5,
              "mandatory": false,
              "readOnly": false,
              "notNull": false,
              "mapping": {
                "source": "hsqldb_PROJECT",
                "columnName": "EXPECTED_END_DATE",
                "type": "DATE"
              }
            }
          }
        },
        {
          "name": "Department",
          "mapping": {
            "sourceTables": [
              {
                "name": "hsqldb_DEPARTMENT",
                "dataSource": "hsqldb",
                "tableName": "DEPARTMENT",
                "primaryKey": [
                  "ID"
                ]
              }
            ]
          },
          "externalKey": [
            "id"
          ],
          "properties": {
            "id": {
              "include": true,
              "type": "STRING",
              "ordinalPosition": 1,
              "mandatory": false,
              "readOnly": false,
              "notNull": false,
              "mapping": {
                "source": "hsqldb_DEPARTMENT",
                "columnName": "ID",
                "type": "VARCHAR"
              }
            },
            "departmentName": {
              "include": true,
              "type": "STRING",
              "ordinalPosition": 2,
              "mandatory": false,
              "readOnly": false,
              "notNull": false,
              "mapping": {
                "source": "hsqldb_DEPARTMENT",
                "columnName": "DEPARTMENT_NAME",
                "type": "VARCHAR"
              }
            },
            "location": {
              "include": true,
              "type": "STRING",
              "ordinalPosition": 3,
              "mandatory": false,
              "readOnly": false,
              "notNull": false,
              "mapping": {
                "source": "hsqldb_DEPARTMENT",
                "columnName": "LOCATION",
                "type": "VARCHAR"
              }
            }
          }
        },
        {
          "name": "Employee",
          "mapping": {
            "sourceTables": [
              {
                "name": "hsqldb_EMPLOYEE",
                "dataSource": "hsqldb",
                "tableName": "EMPLOYEE",
                "primaryKey": [
                  "ID"
                ]
              }
            ]
          },
          "externalKey": [
            "id"
          ],
          "properties": {
            "id": {
              "include": true,
              "type": "STRING",
              "ordinalPosition": 1,
              "mandatory": false,
              "readOnly": false,
              "notNull": false,
              "mapping": {
                "source": "hsqldb_EMPLOYEE",
                "columnName": "ID",
                "type": "VARCHAR"
              }
            },
            "firstName": {
              "include": true,
              "type": "STRING",
              "ordinalPosition": 2,
              "mandatory": false,
              "readOnly": false,
              "notNull": false,
              "mapping": {
                "source": "hsqldb_EMPLOYEE",
                "columnName": "FIRST_NAME",
                "type": "VARCHAR"
              }
            },
            "lastName": {
              "include": true,
              "type": "STRING",
              "ordinalPosition": 3,
              "mandatory": false,
              "readOnly": false,
              "notNull": false,
              "mapping": {
                "source": "hsqldb_EMPLOYEE",
                "columnName": "LAST_NAME",
                "type": "VARCHAR"
              }
            },
            "salary": {
              "include": true,
              "type": "DOUBLE",
              "ordinalPosition": 4,
              "mandatory": false,
              "readOnly": false,
              "notNull": false,
              "mapping": {
                "source": "hsqldb_EMPLOYEE",
                "columnName": "SALARY",
                "type": "DOUBLE"
              }
            },
            "email": {
              "include": true,
              "type": "STRING",
              "ordinalPosition": 5,
              "mandatory": false,
              "readOnly": false,
              "notNull": false,
              "mapping": {
                "source": "hsqldb_EMPLOYEE",
                "columnName": "EMAIL",
                "type": "VARCHAR"
              }
            },
            "department": {
              "include": true,
              "type": "STRING",
              "ordinalPosition": 6,
              "mandatory": false,
              "readOnly": false,
              "notNull": false,
              "mapping": {
                "source": "hsqldb_EMPLOYEE",
                "columnName": "DEPARTMENT",
                "type": "VARCHAR"
              }
            }
          }
        }
      ],
      "edges": [
        {
          "HasDepartment": {
            "isLogical": false,
            "mapping": [
              {
                "fromTable": "EMPLOYEE",
                "fromColumns": [
                  "DEPARTMENT"
                ],
                "toTable": "DEPARTMENT",
                "toColumns": [
                  "ID"
                ],
                "direction": "direct"
              }
            ],
            "properties": {}
          }
        },
        {
          "EmployeeProject": {
            "isLogical": false,
            "mapping": [
              {
                "fromTable": "EMPLOYEE",
                "fromColumns": [
                  "ID"
                ],
                "toTable": "PROJECT",
                "toColumns": [
                  "ID"
                ],
                "joinTable": {
                  "tableName": "EMPLOYEE_PROJECT",
                  "fromColumns": [
                    "EMPLOYEE_ID"
                  ],
                  "toColumns": [
                    "PROJECT_ID"
                  ]
                },
                "direction": "direct"
              }
            ],
            "properties": {
              "role": {
                "include": true,
                "type": "STRING",
                "ordinalPosition": 1,
                "mandatory": false,
                "readOnly": false,
                "notNull": false,
                "mapping": {
                  "source": "hsqldb_EMPLOYEE_PROJECT",
                  "columnName": "ROLE",
                  "type": "VARCHAR"
                }
              }
            }
          }
        }
      ]
    }
  }

}

angular.module('teleporter.components', []).directive(
  `teleporter`,
  downgradeComponent({component: TeleporterComponent}));


export {TeleporterComponent};
