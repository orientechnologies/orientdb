import {Component} from '@angular/core';


import * as $ from "jquery"

import {downgradeComponent} from '@angular/upgrade/static';
import {EtlService} from '../../core/services';
import {AgentService} from "../../core/services/agent.service";

declare var angular:any;

@Component({
  selector: 'etl',
  templateUrl: "./etl.component.html",
  styleUrls: []
})

class EtlComponent {
  private configParams; // Parameters stored during the configuration
  private jsonPrototypes; // Ready to use prototypes to build the partial json

  private finalJson; // The final json, it will be passed to the launch function

  private sourceJson; // The source json
  private extractorJson; // The extractor json
  private transformerJson; // An array containing all transformers json
  private loaderJson; // The loader json

  // Different types, used in the multiple choice dialogs
  private sourceTypes;
  private loaderTypes;
  private URLMethods;
  private predefinedFormats;
  private unresolvedLinkActions;
  private linkFieldTypes;

  private step;
  private hints;


  constructor(private agentService: AgentService, private etlService : EtlService){

    this.init();

    /*this.agentService.isActive().then(() => {
      this.init();
    }); */ // TODO activate enterprise control
  }

  init() {

    // Params to build the partial json.
    this.configParams = {

      // Source
      source: "",
      fileURL: "",
      URLMethod: "GET",
      filePath: "",
      fileLock: false,

      // Extractor
      extractorType: "",
      // Row
      multiline: true,
      linefeed: "\r\n",
      // Csv
      separator: ",",
      columnsOnFirstLine: true,
      columns: [],
      nullValue: "NULL",
      dateFormat: "yyyy-MM-dd",
      dateTimeFormat: "yyyy-MM-dd HH:mm",
      quote: "\"",
      skipFrom: "",
      skipTo: "",
      ignoreEmptyLines: false,
      ignoreMissingColumns: false,
      predefinedFormat: "",
      // JDBC
      driver: "", // mandatory
      url: "", // mandatory
      userName: "", // mandatory
      userPassword: "", // mandatory
      query: "", // mandatory
      queryCount: "",
      // Json has no parameters
      // XML
      rootNode: "",
      tagsAsAttribute: [],

      // Transformer
      transformerType: "",
      customLabel: "",
      // Field
      fieldName: "",
      expression: "", // mandatory
      value: "",
      operation: "set",
      save: false,
      // Merge
      joinFieldName: "", // mandatory
      lookup: "", // mandatory
      unresolvedLinkAction: "NOTHING",
      // Vertex
      class: "V",
      skipDuplicates: false,
      // Edge
      // joinFieldName: "", TODO rename duplicates
      direction: "out",
      // class: "E", TODO rename duplicates
      // lookup: "" TODO rename duplicates
      targetVertexFields: {},
      edgeFields: {},
      // skipDuplicates: false, TODO rename duplicates
      // unresolvedLinkAction: "NOTHING", TODO rename duplicates
      // Flow
      if: "", // mandatory
      // operation: "", // mandatory TODO rename and decide if adopt this transformer
      // Code
      language: "javascript",
      code: "", // Mandatory
      // Link
      // joinFieldName: "", TODO rename duplicates
      // joinValue: "", TODO rename duplicates
      linkFieldName: "", // mandatory
      linkFieldType: "", // mandatory
      // lookup: "", TODO rename duplicates
      // unresolvedLinkAction: "NOTHING", TODO rename duplicates
      // Log
      prefix: "",
      postfix: "",
      // Block
      // Command
      // language: "sql", TODO rename duplicates
      command: "", // mandatory

      // Loader
      loaderType: "",
      // OrientDB
      dbURL: "", // mandatory
      dbUser: "admin",
      dbPassword: "admin",
      serverUser: "root",
      serverPassword: "",
      dbAutoCreate: true,
      dbAutoCreateProperties: false,
      dbAutoDropIfExists: false,
      tx: false,
      txUseLog: false,
      wal: true,
      batchCommit: 0,
      dbType: "document", // or graph
      // class: "", TODO rename duplicates
      cluster: "",
      // classes: TODO ???
      // indexes: TODO ???
      useLightweightEdges: false,
      standardElementConstraints: true

    }

    // Types for sources, extractors, transformers and loaders
    this.sourceTypes = ["jdbc", "local file", "url"];

    this.loaderTypes = ["debug", "orientdb"];

    // Specific types
    this.URLMethods = ["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "TRACE"];
    this.predefinedFormats = ["Default", "Excel", "MySQL", "RCF4180", "TDF"];
    this.unresolvedLinkActions = ["NOTHING", "WARNING", "ERROR", "HALT", "SKIP"];
    this.linkFieldTypes = ["LINK", "LINKSET", "LINKLIST"];

    // User support
    this.step = 1;
    this.hints = {
      // Main hints
      sourceHint: "This is the source on wich the etl is applied. You can use different sources, such as an URL, a local file or a JDBC connection.",
      extractorHint: "The extractor manages how the data are handled from the provided source.",
      transformerHint: "The transformer modules are executed in a pipeline and modify the input data.",
      loaderHint: "The loader is the final part of the process. You can use a debug mode or directly persist your data to OrientDB.",
      executionHint: "Etl is running, and its output is displayed here.",
      // Specific hints
      URLHint: "Defines the URL to look to for source data.",
      URLMethodHint: "Defines the HTTP method to use in extracting data.",
      pathHint: "Defines the path to the local file.",
      lockHint: "Defines whether to lock the file during the extraction phase.",
      customLabelHint: "Use a custom label if you want to use multiple transformers, to distinguish them clearly."

    }

    // Json prototypes, contains partial json
    this.jsonPrototypes = new Map();

    // Source
    this.jsonPrototypes.set("jdbc","{");
    this.jsonPrototypes.set("url","{ source { url: this.configParams.fileURL, method: this.configParams.URLMethod }");
    this.jsonPrototypes.set("file","{");

    // Extractor
    this.jsonPrototypes.set("csv","{");

    // Transformer

    // Loader


  }

  // Getters and setters

  getStep() {
    return this.step;
  }

  setStep(step) {
      this.step = step;
  }

  // Checks and Dinamic creations

  readyForExecution() {
    return true;
  }
  sourceInit() {
    this.sourceJson = this.jsonPrototypes.get(this.configParams.source);

  }

  extractorInit(type) {
    this.configParams.extractorType = type;
    window.alert("It works! You selected " + this.configParams.extractorType);
    $(document).ready(function() {
      $("#addExtractor").click(function() {
        var data = {
          operators: {
            operator: {
              top: 20,
              left: 20,
              properties: {
                title: 'Extractor',
                outputs: {
                  output_1: {
                    label: 'Output 1',
                  },
                }
              }
            }
          }
        };
      });
    });
  }

  transformerInit(type) {

  }

  loaderInit(type) {

  }


  // Core Functions

  launch() {

    this.etlService.launch(this.finalJson).then((data) => {
      this.step = "3";
      this.status();
    }).catch(function (error) {
      alert("Error during etl process!")
    });
  }

  status() {

  }

  // Misc

  scrollLogAreaDown() {
    var logArea = $("#logArea");
    logArea.scrollTop(9999999);
  }

  ngAfterViewChecked() {
    this.enablePopovers();
  }

  enablePopovers() {
    (<any>$('[data-toggle="popover"]')).popover({
      // title: 'About this parameter',
      placement: 'right',
      trigger: 'hover'
    });
  }

}

angular.module('etl.component', []).directive(
  `etl`,
  downgradeComponent({component: EtlComponent}));


export {EtlComponent};
