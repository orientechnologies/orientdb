import {Component} from '@angular/core';

import * as $ from "jquery"
import * as jqueryUI from "jquery-ui";

import "../../util/diagram-editor/jquery.flowchart.min.js";
import "../../util/diagram-editor/jquery.flowchart.min.css";
import "../../util/diagram-editor/flowchart.jquery.json";
import "../../util/diagram-editor/package.json";

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
  private source;
  private extractor;
  private transformer; // Single transformer
  private transformers; // Array containing every transformer
  private loader;

  private finalJson; // The final json, it will be passed to the launch function

  // Different types, used in the multiple choice dialogs
  private sourceTypes;
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
      joinFieldNameEdge: "",
      direction: "out",
      classEdge: "E",
      lookupEdge: "",
      targetVertexFields: {},
      edgeFields: {},
      skipDuplicatesEdge: false,
      unresolvedLinkActionEdge: "NOTHING",
      // Flow
      if: "", // mandatory
      operationFlow: "", // mandatory
      // Code // TODO probably unadoptable
      language: "javascript",
      code: "", // Mandatory
      // Link
      joinFieldNameLink: "",
      joinValueLink: "",
      linkFieldName: "", // mandatory
      linkFieldType: "", // mandatory
      lookupLink: "",
      unresolvedLinkActionLink: "NOTHING",
      // Log
      prefix: "",
      postfix: "",
      // Block
      // Command
      languageCommand: "sql",
      command: "", // mandatory

      // Loader
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
      classOrient: "",
      cluster: "",
      // classes: TODO ???
      // indexes: TODO ???
      useLightweightEdges: false,
      standardElementConstraints: true

    }

    // Types
    this.sourceTypes = ["jdbc", "local file", "url"];
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
    this.step = 2;

    if(this.configParams.source == "jdbc") this.source = null

    if(this.configParams.source == "local file")
      this.source = {
        file: {
          path: this.configParams.filePath,
          lock: this.configParams.fileLock,
          encoding: "UTF-8"
        }
    }

    if(this.configParams.source == "url") {
      this.source = {
        http: {
          url: this.configParams.fileURL,
          method: this.configParams.URLMethod,
          headers: {
            "User-Agent": ""
          }
        }
      }
    }

  }

  extractorInit(type) {
    $("#createExtractor").hide();
    $("#pleaseExtractor").hide();

    if(type == "row") {
      this.extractor = {
        row: {
          multiline: this.configParams.multiline,
          linefeed: this.configParams.linefeed
        }
      }
    }

    if(type == "csv") {
      this.extractor = {
        csv: {
          separator: this.configParams.separator,
          columnsOnFirstLine: this.configParams.columnsOnFirstLine,
          columns: this.configParams.columns,
          nullValue: this.configParams.nullValue,
          dateFormat: this.configParams.dateFormat,
          dateTimeFormat: this.configParams.dateTimeFormat,
          quote: this.configParams.quote,
          skipFrom: this.configParams.skipFrom,
          skipTo: this.configParams.skipTo,
          ignoreEmptyLines: this.configParams.ignoreEmptyLines,
          ignoreMissingColumns: this.configParams.ignoreMissingColumns,
          predefinedFormat: this.configParams.predefinedFormat
        }
      }
    }

    if(type == "jdbc") {
      this.extractor = {
        jdbc: {
          driver: this.configParams.driver,
          url: this.configParams.url,
          userName: this.configParams.userName,
          userPassword: this.configParams.userPassword,
          query: this.configParams.query,
          queryCount: this.configParams.queryCount
        }
      }
    }

    if(type == "json") {
      this.extractor = {
        json: {}
      }
    }

    if(type == "xml") {
      this.extractor = {
        xml: {
          rootNode: this.configParams.rootNode,
          tagsAsAttribute: this.configParams.tagsAsAttribute
        }
      }
    }

  }

  transformerInit(type) {
    $("#pleaseTransformer").hide();

    if(type == "field") {
      this.transformer = {
        field: {
          fieldName: this.configParams.fieldName,
          expression: this.configParams.expression,
          value: this.configParams.value,
          operation: this.configParams.operation,
          save: this.configParams.save
        }
      }
    }

    if(type == "merge") {
      this.transformer = {
        merge: {
          joinFieldName: this.configParams.joinFieldName,
          lookup: this.configParams.lookup,
          unresolvedLinkAction: this.configParams.unresolvedLinkAction
        }
      }
    }

    if(type == "vertex") {
      this.transformer = {
        vertex: {
          class: this.configParams.class,
          skipDuplicates: this.configParams.skipDuplicates
        }
      }
    }

    if(type == "code") {
      this.transformer = {
        code: {
          language: this.configParams.language,
          code: this.configParams.code,
        }
      }
    }

    if(type == "link") {
      this.transformer = {
        link: {
          joinFieldName: this.configParams.joinFieldNameLink,
          joinValue: this.configParams.joinValueLink,
          linkFieldName: this.configParams.linkFieldName,
          linkFieldType: this.configParams.linkFieldType,
          lookup: this.configParams.lookupLink,
          unresolvedLinkAction: this.configParams.unresolvedLinkActionLink
        }
      }
    }

    if(type == "edge") {
      this.transformer = {
        edge: {
          joinFieldName: this.configParams.joinFieldNameEdge,
          direction: this.configParams.direction,
          class: this.configParams.classEdge,
          lookup: this.configParams.lookupEdge,
          targetVertexFields: this.configParams.targetVertexFields,
          edgeFields: this.configParams.edgeFields,
          skipDuplicates: this.configParams.skipDuplicatesEdge,
          unresolvedLinkAction: this.configParams.unresolvedLinkActionEdge
        }
      }
    }

    if(type == "flow") {
      this.transformer = {
        flow: {
          if: this.configParams.if,
          operation: this.configParams.operationFlow
        }
      }
    }

    if(type == "log") {
      this.transformer = {
        log: {
          prefix: this.configParams.prefix,
          postfix: this.configParams.postfix
        }
      }
    }

    /*if(type == "block") {
      this.transformer = {
        block: {

        }
      }
    }*/

    if(type == "command") {
      this.transformer = {
        command: {
          language: this.configParams.languageCommand,
          command: this.configParams.command
        }
      }
    }

    this.transformers.push(this.transformer);
  }

  loaderInit(type) { // TODO a Flowchart node should be displayed, with the loader type. Same for other modules.
    $("#pleaseLoader").hide();
    $("#createLoader").hide();

    if(type == "log") {
      this.loader = {
        log: {}
      }
    }

    if(type == "OrientDB") {
      this.loader = {
        OrientDB: {
          dbURL: this.configParams.dbURL,
          dbUser: this.configParams.dbUser,
          dbPassword: this.configParams.dbPassword,
          serverUser: this.configParams.serverUser,
          serverPassword: this.configParams.serverPassword,
          dbAutoCreate: this.configParams.dbAutoCreate,
          dbAutoCreateProperties: this.configParams.dbAutoCreateProperties,
          dbAutoDropIfExists: this.configParams.dbAutoDropIfExists,
          tx: this.configParams.tx,
          txUseLog: this.configParams.txUseLog,
          wal: this.configParams.wal,
          batchCommit: this.configParams.batchCommit,
          dbType: this.configParams.dbType,
          class: this.configParams.classOrient,
          cluster: this.configParams.cluster,
          // classes: this.configParams.classes,
          // indexes: this.configParams.indexes,
          useLightweightEdges: this.configParams.useLightweightEdges,
          standardElementConstraints: this.configParams.standardElementConstraints
        }
      }
    }
  }

  deleteExtractor() {
    this.extractor = null;
  }

  deleteTransformer(name) {

  }

  deleteLoader() {
    this.loader = null;
  }


  // Core Functions

  generateTransformersList(transformers) {
    var transformersList = {
      placeholder: "test"
    };

    return transformers;
  }

  launch() {
    var final = {};
    $.extend(final, this.source, this.extractor, this.generateTransformersList(this.transformers), this.loader);
    this.finalJson = JSON.stringify(final);

    this.step = "3";

   // this.etlService.launch(this.finalJson).then((data) => {
   //   this.step = "3";
   //   this.status();
   // }).catch(function (error) {
   //   alert("Error during etl process!")
   // });
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
