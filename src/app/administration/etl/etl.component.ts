import * as $ from "jquery"
import * as jqueryUI from "jquery-ui";

import "../../util/diagram-editor/jquery.flowchart.min.js";
import "../../util/diagram-editor/jquery.flowchart.min.css";
import "../../util/diagram-editor/flowchart.jquery.json";
import "../../util/diagram-editor/package.json";

import {Component} from '@angular/core';

import {downgradeComponent} from "@angular/upgrade/static";
import {EtlService} from "../../core/services";
import {AgentService} from "../../core/services/agent.service";
import {ObjectKeysPipe} from "../../core/pipes"

declare var angular:any;

@Component({
  selector: 'etl',
  templateUrl: "./etl.component.html",
  styleUrls: []
})

class EtlComponent {
  private sourcePrototype;
  private extractorPrototype;
  private transformerPrototype;
  private loaderPrototype;
  private source = undefined; // Source variable
  private extractor = undefined; // Extractor variable
  private currentTransformer = undefined; // Current transformer
  private transformers = []; // Array containing every transformer
  private loader = undefined; // Loader variable

  // Types needed for controls
  private extractorType;
  private currentTransformerType;
  private loaderType;

  // Keys to iterate TODO try a pipe-style implementation
  private eKeys;
  private tKeys;
  private lKeys;

  private finalJson; // The final json, it will be passed to the launch function

  private step;
  private hints;
  private ready;


  constructor(private agentService: AgentService, private etlService : EtlService){

    this.init();

    /*this.agentService.isActive().then(() => {
     this.init();
     }); */ // TODO activate enterprise control
  }

  init() {
    this.sourcePrototype = {
      source: {
        value: undefined,
        types: ["jdbc", "local file", "url", "upload"] // TODO uploader library
      },
      fileURL: undefined,
      URLMethod: {
        value: undefined,
        types: ["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "TRACE"]
      },
      filePath: undefined,
      fileLock: false
    }

    this.extractorPrototype = {
      row: {
        multiline: {
          mandatory: false,
          value:true,
          types: [true, false]
        },
        linefeed: {
          mandatory: false,
          value: "\r\n"
        },
      },

      csv: {
        separator: {
          mandatory:false,
          value: ","
        },
        columnsOnFirstLine: {
          mandatory: false,
          value:true,
          types: [true, false]
        },
        columns: {
          mandatory: false,
          value: undefined
        },
        nullValue: {
          mandatory:false,
          value: "NULL"
        },
        dateFormat: {
          mandatory: false,
          value: "yyyy-MM-dd"
        },
        dateTimeFormat: {
          mandatory: false,
          value: "yyyy-MM-dd HH:mm"
        },
        quote: {
          mandatory: false,
          value: '"'
        },
        skipFrom: {
          mandatory: false,
          value: undefined
        },
        skipTo: {
          mandatory: false,
          value: undefined
        },
        ignoreEmptyLines: {
          mandatory: false,
          value: false,
          types: [true, false]
        },
        ignoreMissingColumns: {
          mandatory: false,
          value: false,
          types: [true, false]
        },
        predefinedFormat: {
          mandatory: false,
          value: "Default",
          types: ["Default", "Excel", "MySQL", "RCF4180", "TDF"]
        }
      },

      jdbc: {
        driver: {
          mandatory: true,
          value: undefined
        },
        url: {
          mandatory: true,
          value: undefined
        },
        userName: {
          mandatory:true,
          value: "admin"
        },
        userPassword: {
          mandatory: true,
          value: undefined
        },
        query: {
          mandatory: true,
          value: undefined
        },
        queryCount: {
          mandatory: false,
          value: undefined,
        }
      },

      // Json has no parameters

      xml: {
        rootNode: {
          mandatory: false,
          value: undefined
        },
        tagsAsAttribute: {
          mandatory: false,
          value:[]
        }
      }

    }

    this.transformerPrototype = {
      customLabel: undefined,

      field: {
        fieldName: {
          mandatory: false,
          value: undefined
        },
        expression: {
          mandatory: true,
          value: undefined
        },
        value: {
          mandatory: false,
          value: undefined
        },
        operation: {
          mandatory: false,
          value: "SET",
          types: ["SET", "REMOVE"]
        },
        save: {
          mandatory: false,
          value: false,
          types: [true, false]
        }
      },

      merge: {
        joinFieldName: {
          mandatory: true,
          value: undefined
        },
        lookup: {
          mandatory: true,
          value: undefined
        },
        unresolvedLinkAction: {
          mandatory: false,
          value: "NOTHING",
          types: ["NOTHING", "WARNING", "ERROR", "HALT", "SKIP"]
        }
      },

      vertex: {
        class: {
          mandatory: false,
          value: "V"
        },
        skipDuplicates: {
          mandatory: false,
          value: false,
          types: [true, false]
        }
      },

      edge: {
        joinFieldName: {
          mandatory: true,
          value: undefined
        },
        direction: {
          mandatory: false,
          value: "out",
          types: ["in", "out"]
        },
        class: {
          mandatory: false,
          value: "E"
        },
        lookup: {
          mandatory: true,
          value: undefined
        },
        targetVertexFields: {
          mandatory: false,
          value: undefined
        },
        edgeFields: {
          mandatory: false,
          value: undefined
        },
        skipDuplicates: {
          mandatory: false,
          value: false,
          types: [true, false]
        },
        unresolvedLinkAction: {
          mandatory: false,
          value: "NOTHING",
          types: ["NOTHING", "WARNING", "ERROR", "HALT", "SKIP"]
        },
      },

      flow: {
        if: {
          mandatory: true,
          value: undefined
        },
        operation: {
          mandatory: true,
          value: "skip",
          types: ["skip", "halt"]
        },
      },

      code: { // TODO probably unadoptable
        language: {
          mandatory: false,
          value: "JavaScript"
        },
        code: {
          mandatory: true,
          value: undefined
        },
      },

      link: {
        joinFieldName: {
          mandatory: false,
          value: undefined
        },
        joinValue: {
          mandatory: false,
          value: undefined
        },
        linkFieldName: {
          mandatory: true,
          value: undefined
        },
        linkFieldType: {
          mandatory: true,
          value: undefined,
          types: ["LINK", "LINKSET", "LINKLIST"]
        },
        lookup: {
          mandatory: true,
          value: undefined
        },
        unresolvedLinkAction: {
          mandatory: false,
          value: "NOTHING",
          types: ["NOTHING", "WARNING", "ERROR", "HALT", "SKIP"]
        },
      },

      log: {
        prefix: {
          mandatory: false,
          value:undefined
        },
        postfix: {
          mandatory: false,
          value: undefined
        }
      },

      // Block has no parameters

      command: {
        language: {
          mandatory: false,
          value: "sql",
          types: ["sql","gremlin"]
        },
        command: {
          mandatory: true,
          value: undefined
        },
      }

    }

    this.loaderPrototype = {
      // Log has no parameters

      orientDb: {
        dbURL: {
          mandatory: true,
          value: undefined
        },
        dbUser: {
          mandatory: false,
          value: "admin"
        },
        dbPassword: {
          mandatory: false,
          value: "admin"
        },
        serverUser: {
          mandatory: false,
          value: "root"
        },
        serverPassword: {
          mandatory: false,
          value: ""
        },
        dbAutoCreate: {
          mandatory: false,
          value: true,
          types: [true, false]
        },
        dbAutoCreateProperties: {
          mandatory: false,
          value: false,
          types: [true, false]
        },
        dbAutoDropIfExists: {
          mandatory: false,
          value: false,
          types: [true, false]
        },
        tx: {
          mandatory: false,
          value: false,
          types: [true, false]
        },
        txUseLog: {
          mandatory: false,
          value: false,
          types: [true, false]
        },
        wal: {
          mandatory: false,
          value: true,
          types: [true, false]
        },
        batchCommit: {
          mandatory: false,
          value: 0
        },
        dbType: {
          mandatory: false,
          value: "document",
          types: ["document", "graph"]
        },
        class: {
          mandatory: false,
          value: undefined
        },
        cluster: {
          mandatory: false,
          value: undefined
        },
        classes: {
          mandatory: false,
          value:undefined
        },
        indexes: {
          mandatory: false,
          value: undefined
        },
        useLightweightEdges: {
          mandatory: false,
          value: false,
          types: [true, false]
        },
        standardElementConstraints: {
          mandatory: false,
          value: true,
          types: [true, false]
        }
      }

    }

    // User support
    this.ready = false;
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
      customLabelHint: "Use a custom label if you want to use multiple transformers, to distinguish them clearly.",
      extractor: {
        multiline: "Defines if multiline is supported",
        linefeed: "Defines the linefeed to use in the event of multiline processing",
        separator: "Defines the column separator",
        columnsOnFirstLine: "Defines whether the first line contains column descriptors",
        columns: "Defines an array for names and (optionally) types to write",
        nullValue: "Defines the null value in the file",
        dateFormat: "Defines the format to use in parsing dates from file",
        dateTimeFormat: "Defines the format to use in parsing dates from file",
        quote: "Defines string character delimiter",
        skipFrom: "Defines the line number you want to skip from",
        skipTo: "Defines the line number you want to skip to",
        ignoreEmptyLines: "Defines whether it should ignore empty lines",
        ignoreMissingColumns: "Defines whether it should ignore empty columns",
        predefinedFormat: "Defines the csv format you want to use",
        driver: "Defines the JDBC Driver class",
        url: "Defines the JDBC URL to connect to",
        userName: "Defines the username to use on the source database",
        userPassword: "Defines the user password to use on the source database",
        query: "Defines the query to extract the record you want to import",
        queryCount: "Defines query that returns the count of the fetched record (used to provide a correct progress indication)",
        rootNode: "Defines the root node to extract in the XML, By default, it builds from the root element in the file",
        tagsAsAttribute: "Defines an array of elements, where child elements are considered as attributes of the document and the attribute values as the text within the element"
      },
      transformer: {
        fieldName: "Defines the document field name to use",
        expression: "Defines the expression you want to evaluate, using OrientDB SQL",
        value: "Defines the value to set. If the value is taken or computed at run-time, use 'expression' instead",
        if: "Condition for the operation to be executed",
        operation: "Defines the operation to execute",
        save: "Defines whether to save the vertex, edge or document right after setting the fields",
        joinFieldName: "Defines the field containing the join value",
        lookup: "Defines the index on which to execute the lookup, or a select query",
        unresolvedLinkAction: "Defines the action to execute in the event that the join hasn't been resolved",
        class: "Defines the class to use",
        skipDuplicates: "Defines whether it skips duplicates",
        direction: "Defines the edge direction",
        targetVertexFields: "Defines the field on which to set the target vertex",
        edgeFields: "Defines the fields to set in the edge",
        language: "Defines the programming language to use",
        code: "Defines the code to execute",
        joinValue: "Defines the value to lookup",
        linkFieldName: "Defines the field containing the link to set",
        linkFieldType: "Defines the link type",
        prefix: "Defines what it writes before the content",
        postfix: "Defines what it writes after the content",
        command: "Defines the command to execute"
      },
      loader: {
        dbURL: "Defines the database URL",
        dbUser: "Defines the user name",
        dbPassword: "Defines the user password",
        serverUser: "Defines the server administrator user name, usually root",
        serverPassword: "Defines the server administrator user password that is provided at server startup",
        dbAutoCreate: "Defines whether it automatically creates the database, in the event that it doesn't exist already",
        dbAutoCreateProperties: "Defines whether it automatically creates properties in the schema",
        dbAutoDropIfExists: "Defines whether it automatically drops the database if exists already",
        tx: "Defines whether it uses transactions",
        txUseLog: "Defines whether it uses log in transactions",
        wal: "Defines whether it uses write ahead logging. disable to achieve better performance",
        batchCommit: "When using tansactions, defines the batch of entries it commits. Helps avoid having one large transaction in memory",
        dbType: "Defines the database type, graph or document",
        class: "Defines the class to use in storing new record",
        cluster: "Defines the cluster in which to store the new record",
        classes: "Defines whether it creates classes, if not defined already in the database",
        indexes: "Defines indexes to use on the ETL process. Before starting, it creates any declared indexes not present in the database. Indexes must have 'type', 'class' and 'fields'",
        useLightweightEdges: "Defines whether it changes the default setting for Lightweight Edges",
        standardElementConstraints: "Defines whether it changes the default setting for TinkerPop Blueprint constraints. Value cannot be null and you cannot use id as a property name"
      }
    }
  }

  // Getters and setters

  getStep() {
    return this.step;
  }

  setStep(step) {
    this.step = step;
  }

  // Dynamic creations

  sourceInit() {
    this.step = 2;

    if(this.sourcePrototype.source.value === "jdbc") this.source = null

    if(this.sourcePrototype.source.value === "local file")
      this.source = {
        file: {
          path: this.sourcePrototype.filePath,
          lock: this.sourcePrototype.fileLock,
          encoding: "UTF-8"
        }
      }

    if(this.sourcePrototype.source.value === "url") {
      this.source = {
        http: {
          url: this.sourcePrototype.fileURL,
          method: this.sourcePrototype.URLMethod.value,
          headers: {
            "User-Agent": ""
          }
        }
      }
    }

  }

  extractorInit(type) {
    // Variable creation
    if(type === "row")
      this.extractor = {
        row: {
          multiline: this.extractorPrototype.row.multiline.value,
          linefeed: this.extractorPrototype.row.linefeed.value
        }
      }

    if(type === "csv")
      this.extractor = {
        csv: {
          separator: this.extractorPrototype.csv.separator.value,
          columnsOnFirstLine: this.extractorPrototype.csv.columnsOnFirstLine.value,
          columns: this.extractorPrototype.csv.columns.value,
          nullValue: this.extractorPrototype.csv.nullValue.value,
          dateFormat: this.extractorPrototype.csv.dateFormat.value,
          dateTimeFormat: this.extractorPrototype.csv.dateTimeFormat.value,
          quote: this.extractorPrototype.csv.quote.value,
          skipFrom: this.extractorPrototype.csv.skipFrom.value,
          skipTo: this.extractorPrototype.csv.skipTo.value,
          ignoreEmptyLines: this.extractorPrototype.csv.ignoreEmptyLines.value,
          ignoreMissingColumns: this.extractorPrototype.csv.ignoreMissingColumns.value,
          predefinedFormat: this.extractorPrototype.csv.predefinedFormat.value
        }
      }

    if(type === "jdbc")
      this.extractor = {
        jdbc: {
          driver: this.extractorPrototype.jdbc.driver.value,
          url: this.extractorPrototype.jdbc.url.value,
          userName: this.extractorPrototype.jdbc.userName.value,
          userPassword: this.extractorPrototype.jdbc.userPassword.value,
          query: this.extractorPrototype.jdbc.query.value,
          queryCount: this.extractorPrototype.jdbc.queryCount.value
        }
      }

    if(type === "json")
      this.extractor = {
        json: {}
      }

    if(type === "xml")
      this.extractor = {
        xml: {
          rootNode: this.extractorPrototype.xml.rootNode.value,
          tagsAsAttribute: this.extractorPrototype.xml.tagsAsAttribute.value
        }
      }


    // Flowchart
    /*    $(document).ready(function() { // TODO remove the comment. atm this blocks the instructions that follow.
     var dataExtractor = {
     operators: {
     operator: {
     top: 30,
     left: 70,
     properties: {
     title: type + ' extractor',
     outputs: {
     output_1: {
     label: 'click to configure'
     }
     }
     }
     }
     }
     };

     (<any>$('#extractorSpace')).flowchart({
     data: dataExtractor
     });
     }); */

    this.eKeys = Object.keys(this.extractor[type]);
    this.extractorType = type;
    this.readyForExecution();

    $("#createExtractor").hide();
    $("#pleaseExtractor").hide();

    $("#panelPlaceholder").hide(); // Shows the options for the extractor, hides the placeholder
    $("#loaderOptions").hide();
    $("#transformerOptions").hide();
    $("#extractorOptions").show();
  }

  transformerInit(type) { // Block and Code aren't active atm. Csv is deprecated.
    var transformer;
    // TODO: use the custom label in the flowchart, show the options for the created transformer

    // Variable creation
    if(type === "field")
      transformer = {
        field: {
          fieldName: this.transformerPrototype.field.fieldName.value,
          expression: this.transformerPrototype.field.expression.value,
          value: this.transformerPrototype.field.value,
          operation: this.transformerPrototype.field.operation.value,
          save: this.transformerPrototype.field.save.value
        }
      }

    if(type === "merge")
      transformer = {
        merge: {
          joinFieldName: this.transformerPrototype.merge.joinFieldName.value,
          lookup: this.transformerPrototype.merge.lookup.value,
          unresolvedLinkAction: this.transformerPrototype.merge.unresolvedLinkAction.value
        }
      }

    if(type === "vertex")
      transformer = {
        vertex: {
          class: this.transformerPrototype.vertex.class.value,
          skipDuplicates: this.transformerPrototype.vertex.skipDuplicates.value
        }
      }

    if(type === "code")
      transformer = {
        code: {
          language: this.transformerPrototype.code.language.value,
          code: this.transformerPrototype.code.code.value,
        }
      }

    if(type === "link")
      transformer = {
        link: {
          joinFieldName: this.transformerPrototype.link.joinFieldName.value,
          joinValue: this.transformerPrototype.link.joinValue.value,
          linkFieldName: this.transformerPrototype.link.linkFieldName.value,
          linkFieldType: this.transformerPrototype.link.linkFieldType.value,
          lookup: this.transformerPrototype.link.lookup.value,
          unresolvedLinkAction: this.transformerPrototype.link.unresolvedLinkAction.value
        }
      }

    if(type === "edge")
      transformer = {
        edge: {
          joinFieldName: this.transformerPrototype.edge.joinFieldName.value,
          direction: this.transformerPrototype.edge.direction.value,
          class: this.transformerPrototype.edge.class.value,
          lookup: this.transformerPrototype.edge.lookup.value,
          targetVertexFields: this.transformerPrototype.edge.targetVertexFields.value,
          edgeFields: this.transformerPrototype.edge.edgeFields.value,
          skipDuplicates: this.transformerPrototype.edge.skipDuplicates.value,
          unresolvedLinkAction: this.transformerPrototype.edge.unresolvedLinkAction.value
        }
      }

    if(type === "flow")
      transformer = {
        flow: {
          if: this.transformerPrototype.flow.if.value,
          operation: this.transformerPrototype.flow.operation.value
        }
      }

    if(type === "log")
      transformer = {
        log: {
          prefix: this.transformerPrototype.log.prefix.value,
          postfix: this.transformerPrototype.log.postfix.value
        }
      }

    if(type === "block")
      transformer = {
        block: {

        }
      }

    if(type === "command")
      transformer = {
        command: {
          language: this.transformerPrototype.command.language.value,
          command: this.transformerPrototype.command.command.value
        }
      }


    // Flowchart
    /*  $(document).ready(function() {
     var dataTransformer = {
     operators: {
     operator: {
     top: 20,
     left: 160,
     properties: {
     title: type + ' transformer',
     inputs: {
     input1: {
     label: ""
     }
     },
     outputs: {
     output_1: {
     label: 'click to configure'
     }
     }
     }
     }
     }
     };

     (<any>$('#transformerSpace')).flowchart({
     data: dataTransformer
     });
     }); */

    // Push into the array
    this.transformers.push(transformer);
    this.currentTransformerType = type;
    this.currentTransformer = transformer;
    this.tKeys = Object.keys(this.currentTransformer[type]);
    this.readyForExecution();

    $("#pleaseTransformer").hide();

    $("#panelPlaceholder").hide(); // Shows the options for the extractor, hides the placeholder
    $("#loaderOptions").hide();
    $("#extractorOptions").hide();
    $("#transformerOptions").show();

  }

  loaderInit(type) {
    // Variable creation
    if(type === "log")
      this.loader = {
        log: {}
      }

    if(type === "orientDb")
      this.loader = {
        orientDb: {
          dbURL: this.loaderPrototype.orientDb.dbURL.value,
          dbUser: this.loaderPrototype.orientDb.dbUser.value,
          dbPassword: this.loaderPrototype.orientDb.dbPassword.value,
          serverUser: this.loaderPrototype.orientDb.serverUser.value,
          serverPassword: this.loaderPrototype.orientDb.serverPassword.value,
          dbAutoCreate: this.loaderPrototype.orientDb.dbAutoCreate.value,
          dbAutoCreateProperties: this.loaderPrototype.orientDb.dbAutoCreateProperties.value,
          dbAutoDropIfExists: this.loaderPrototype.orientDb.dbAutoDropIfExists.value,
          tx: this.loaderPrototype.orientDb.tx.value,
          txUseLog: this.loaderPrototype.orientDb.txUseLog.value,
          wal: this.loaderPrototype.orientDb.wal.value,
          batchCommit: this.loaderPrototype.orientDb.batchCommit.value,
          dbType: this.loaderPrototype.orientDb.dbType.value,
          class: this.loaderPrototype.orientDb.class.value,
          cluster: this.loaderPrototype.orientDb.cluster.value,
          classes: this.loaderPrototype.orientDb.classes.value,
          indexes: this.loaderPrototype.orientDb.indexes.value,
          useLightweightEdges: this.loaderPrototype.orientDb.useLightweightEdges.value,
          standardElementConstraints: this.loaderPrototype.orientDb.standardElementConstraints.value
        }
      }

    // Flowchart
    /*    $(document).ready(function() {
     var dataLoader = {
     operators: {
     operator: {
     top: 30,
     left: 40,
     properties: {
     title: type + ' loader',
     inputs: {
     input1: {
     label: 'click to configure'
     }
     }
     }
     }
     }
     };

     (<any>$('#loaderSpace')).flowchart({
     data: dataLoader
     });
     }); */

    this.lKeys = Object.keys(this.loader[type]);
    this.loaderType = type;
    this.readyForExecution();

    $("#pleaseLoader").hide();
    $("#createLoader").hide();

    $("#panelPlaceholder").hide(); // Shows the options for the extractor, hides the placeholder
    $("#extractorOptions").hide();
    $("#transformerOptions").hide();
    $("#loaderOptions").show();

  }

  deleteExtractor() {
    this.extractor = undefined;
    this.extractorType = undefined;

    // Jquery hide/show
    $("#createExtractor").show();
    $("#pleaseExtractor").show();
    $("#extractorOptions").hide();
    $("#panelPlaceholder").show();
  }

  deleteTransformer() {
    var index = this.transformers.indexOf(this.currentTransformer);
    if (index > -1) {
      this.transformers.splice(index, 1);
    }
    this.currentTransformer = undefined;
    this.currentTransformerType = undefined;

    // Jquery hide/show
    $("#transformerOptions").hide();
    $("#panelPlaceholder").show();
    if(this.transformers.length == 0) $("#pleaseTransformer").show();
  }

  deleteLoader() {
    this.loader = undefined;
    this.loaderType = undefined;

    // Jquery hide/show
    $("#createLoader").show();
    $("#pleaseLoader").show();
    $("#loaderOptions").hide();
    $("#panelPlaceholder").show();
  }


  // Core Functions

  readyForExecution() {

    /*// If exists at least one module for every type (source excluded)
     if(this.extractor && this.transformers && this.loader) {
     // Controls, for every property of the extractor module, if it's mandatory and has a value
     for(var property in this.extractorPrototype[this.extractorType]) {
     if(this.extractorPrototype[this.extractorType].hasOwnProperty(property)) {
     if(this.extractorPrototype[this.extractorType][property]["mandatory"] && !this.extractor[this.extractorType][property]) {
     this.ready = false;
     return;
     }
     else
     this.ready = true;
     }
     }
     for(var property in this.loaderPrototype[this.loaderType]) {
     if(this.loaderPrototype[this.loaderType].hasOwnProperty(property)) {
     if(this.loaderPrototype[this.loaderType][property]["mandatory"] && !this.loader[this.loaderType][property]) {
     this.ready = false;
     return;
     }
     else
     this.ready = true;
     }
     }
     }*/
    this.ready = true; // TODO reactivate and finish the proper function
  }

  launch() { // TODO maybe even a restart function is needed, to avoid weird behaviors
    if(this.source)
      this.finalJson = '{"source":' + JSON.stringify(this.source) + ',"extractor":' + JSON.stringify(this.extractor) +
        ',"transformers":' + JSON.stringify(this.transformers) + ',"loader":' + JSON.stringify(this.loader) + "}";
    else
      this.finalJson = '{"extractor":' + JSON.stringify(this.extractor) + ',"transformers":' + JSON.stringify(this.transformers) +
        ',"loader":' + JSON.stringify(this.loader) + "}";

    this.step = "3";

    /*
     this.etlService.launch(this.finalJson).then((data) => {
     this.step = "3";
     this.status();
     }).catch(function (error) {
     alert("Error during etl process!")
     });
     */
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
