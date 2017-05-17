import * as $ from "jquery"

import "../../util/diagram-editor/jquery.flowchart.min.js";
import "../../util/diagram-editor/jquery.flowchart.min.css";
import "../../util/diagram-editor/flowchart.jquery.json";
import "../../util/diagram-editor/jquery-ui.min.css";
import "../../util/diagram-editor/jquery-ui.min.js";

import {Component, NgZone} from '@angular/core';

import {downgradeComponent} from "@angular/upgrade/static";
import {EtlService} from "../../core/services";
import {AgentService} from "../../core/services/agent.service";

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
  private source; // Source variable
  private extractor; // Extractor variable
  private currentTransformer; // Current transformer TODO probably removable with a different logic
  private transformers = []; // Array containing every transformer
  private loader; // Loader variable

  // Types needed for controls
  private extractorType;
  private transformerTypes = [];
  private loaderType;

  // Keys to iterate TODO try a pipe-style implementation
  private eKeys;
  private tKeys;
  private lKeys;

  // Flowchart stuff
  private eFlowchart = $("#extractorSpace");
  private tFlowchart;
  private lFlowchart;

  private step;
  private hints;

  private ready;
  private importReady;
  private oldConfig;
  private finalJson; // The final json, it will be passed to the launch function
  private job;
  private jobRunning;

  constructor(private agentService: AgentService, private etlService: EtlService, private zone: NgZone) {

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
      fileLock: false,
      oldConfigLocation: undefined
    }

    this.extractorPrototype = {
      row: {
        multiline: {
          mandatory: false,
          value: true,
          types: [true, false]
        },
        linefeed: {
          mandatory: false,
          value: "\r\n"
        },
      },

      csv: {
        separator: {
          mandatory: false,
          value: ","
        },
        columnsOnFirstLine: {
          mandatory: false,
          value: true,
          types: [true, false]
        },
        columns: {
          mandatory: false,
          value: undefined
        },
        nullValue: {
          mandatory: false,
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
          mandatory: true,
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
          value: []
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
          types: ["out", "in"]
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
          value: "LINK",
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
          value: undefined
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
          types: ["sql", "gremlin"]
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
          value: undefined
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
    this.importReady = false;
    this.job = {};
    this.jobRunning = false;
    this.step = 0;
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

  // Dynamic creations

  sourceInit() {
    this.step = 2;

    if (this.sourcePrototype.source.value === "jdbc") this.source = null

    if (this.sourcePrototype.source.value === "local file")
      this.source = {
        file: {
          path: this.sourcePrototype.filePath,
          lock: this.sourcePrototype.fileLock,
          encoding: "UTF-8"
        }
      }

    if (this.sourcePrototype.source.value === "url") {
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
    if (type === "row")
      this.extractor = {
        row: {
          multiline: this.extractorPrototype.row.multiline.value,
          linefeed: this.extractorPrototype.row.linefeed.value
        }
      }

    if (type === "csv")
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

    if (type === "jdbc")
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

    if (type === "json")
      this.extractor = {
        json: {}
      }

    if (type === "xml")
      this.extractor = {
        xml: {
          rootNode: this.extractorPrototype.xml.rootNode.value,
          tagsAsAttribute: this.extractorPrototype.xml.tagsAsAttribute.value
        }
      }


    // Flowchart
    $(document).ready(function () { // TODO atm this blocks the instructions that follow.
      var dataExtractor = {
        operators: {
          operator: {
            top: 30,
            left: 40,
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

      var eFlowchart = $('#extractorSpace');
      (<any>eFlowchart).flowchart({
        data: dataExtractor,
        onOperatorSelect: function(operatorId) {
          $("#extractorOptions").slideDown(800);
          $("#transformerOptions").slideUp(800);
          $("#loaderOptions").slideUp(800);
          return true;
        },
      });
    });

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
    // TODO: custom labels. Probably another parallel array could be useful

    // Variable creation
    if (type === "field")
      transformer = {
        field: {
          fieldName: this.transformerPrototype.field.fieldName.value,
          expression: this.transformerPrototype.field.expression.value,
          value: this.transformerPrototype.field.value.value,
          operation: this.transformerPrototype.field.operation.value,
          save: this.transformerPrototype.field.save.value
        }
      }

    if (type === "merge")
      transformer = {
        merge: {
          joinFieldName: this.transformerPrototype.merge.joinFieldName.value,
          lookup: this.transformerPrototype.merge.lookup.value,
          unresolvedLinkAction: this.transformerPrototype.merge.unresolvedLinkAction.value
        }
      }

    if (type === "vertex")
      transformer = {
        vertex: {
          class: this.transformerPrototype.vertex.class.value,
          skipDuplicates: this.transformerPrototype.vertex.skipDuplicates.value
        }
      }

    if (type === "code")
      transformer = {
        code: {
          language: this.transformerPrototype.code.language.value,
          code: this.transformerPrototype.code.code.value,
        }
      }

    if (type === "link")
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

    if (type === "edge")
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

    if (type === "flow")
      transformer = {
        flow: {
          if: this.transformerPrototype.flow.if.value,
          operation: this.transformerPrototype.flow.operation.value
        }
      }

    if (type === "log")
      transformer = {
        log: {
          prefix: this.transformerPrototype.log.prefix.value,
          postfix: this.transformerPrototype.log.postfix.value
        }
      }

    if (type === "block")
      transformer = {
        block: {}
      }

    if (type === "command")
      transformer = {
        command: {
          language: this.transformerPrototype.command.language.value,
          command: this.transformerPrototype.command.command.value
        }
      }


    // Flowchart
    $(document).ready(function() {
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

      var $tFlowchart = $('#transformerSpace');
      (<any>$tFlowchart).flowchart({
        data: dataTransformer,
        onOperatorSelect: function(operatorId) {
          $("#transformerOptions").slideDown(800);
          $("#extractorOptions").slideUp(800);
          $("#loaderOptions").slideUp(800);
          return true;
        },
      });
    });

    // Push into the arrays
    this.transformers.push(transformer);
    this.transformerTypes.push(type);
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
    if (type === "log")
      this.loader = {
        log: {}
      }

    if (type === "orientDb")
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
    $(document).ready(function() {
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

      var $lFlowchart = $('#loaderSpace');
      (<any>$lFlowchart).flowchart({
        data: dataLoader,
        onOperatorSelect: function(operatorId) {
          $("#loaderOptions").slideDown(800);
          $("#transformerOptions").slideUp(800);
          $("#extractorOptions").slideUp(800);
          return true;
        },
      });
    });

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

  deleteExtractor() { // TODO delete the flowchart too
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
      this.transformerTypes.splice(index, 1);
    }
    this.currentTransformer = undefined;

    // Jquery hide/show
    $("#transformerOptions").hide();
    $("#panelPlaceholder").show();

    if (this.transformers.length == 0) $("#pleaseTransformer").show();
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

  oldConfigInit(oldConfig) { // TODO draw corresponding flowcharts, test, direct run feature?
    var etl = JSON.parse(oldConfig);

    this.extractor = etl.extractor;
    this.transformers = etl.transformers;
    this.loader = etl.loader;
    this.extractorType = Object.getOwnPropertyNames(etl.extractor)[0];
    for(var i = 0; i < etl.transformers.length; i++) {
      this.transformerTypes.push(Object.getOwnPropertyNames(etl.transformers[i])[0]);
    }
    this.currentTransformer = etl.transformers[etl.transformers.length -1];
    this.loaderType = Object.getOwnPropertyNames(etl.loader)[0];

    // Skip step 1 if the old configuration uses a jdbc source
    if(etl.source) {
      this.source = etl.source;
      this.sourcePrototype.source.value = Object.getOwnPropertyNames(etl.source)[0];
      this.setStep(1);
    }
    else {
      this.sourcePrototype.source.value = "jdbc";
      this.setStep(2);
    }

    // control to eventually activate the run button
    this.readyForExecution();

    // Jquery // TODO move it
    $("#pleaseExtractor").hide();
    $("#pleaseTransformer").hide();
    $("#pleaseLoader").hide();
    $("#createExtractor").hide();
    $("#createLoader").hide();
    $("#panelPlaceholder").hide();
    $("#extractorOptions").show();
  }


  // Core Functions

  readyForExecution() {

    // If one module for every type exists (source excluded)
    if (this.extractor && this.transformers.length > 0 && this.loader) {
      // Controls, for every property of every module, if it's mandatory and has a value
      // Extractor control
      for (var property in this.extractorPrototype[this.extractorType]) {
        if (this.extractorPrototype[this.extractorType].hasOwnProperty(property)) {
          if (this.extractorPrototype[this.extractorType][property]["mandatory"] && !this.extractor[this.extractorType][property]) {
            this.ready = false;
            return;
          }
        }
      }
      // Transformers control
      for (var i = 0; i < this.transformers.length; i++) {
        var type = this.getTransformerType(this.transformers[i]);
        for (var property in this.transformerPrototype[type]) {
          if (this.transformerPrototype[type].hasOwnProperty(property)) {
            if (this.transformerPrototype[type][property]["mandatory"] && !this.transformers[i][type][property]) {
              this.ready = false;
              return;
            }
          }
        }
      }
      // Loader control
      for (var property in this.loaderPrototype[this.loaderType]) {
        if (this.loaderPrototype[this.loaderType].hasOwnProperty(property)) {
          if (this.loaderPrototype[this.loaderType][property]["mandatory"] && !this.loader[this.loaderType][property]) {
            this.ready = false;
            return;
          }
        }
      }
      this.ready = true;
    }
  }

  launch() {
    var etl = {}; // optional source control. Is that needed?

    if(this.source)
      etl = {
        source: this.source,
        extractor: this.extractor,
        transformers: this.transformers,
        loader: this.loader
      };
    else
      etl = {
        extractor: this.extractor,
        transformers: this.transformers,
        loader: this.loader
      };

    this.finalJson = JSON.stringify(etl);

    this.step = "3";

    /*this.etlService.launch(this.finalJson).then((data) => {
     this.step = "3";
     this.jobRunning = true;
     this.status();
     }).catch(function (error) {
     alert("Error during etl process!")
     });*/

  }

  status() {
    if (this.jobRunning) {
      this.etlService.status().then((data) => {
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
        }, 3000);

      });
    }
  }

  reset() {
    // useful assignations to reset the configuration
    this.transformers = [];
    this.currentTransformer = undefined;
    this.extractor = undefined;
    this.loader = undefined;
    this.source = undefined;
    this.extractorType = undefined;
    this.transformerTypes = [];
    this.loaderType = undefined;
    this.eKeys = undefined;
    this.tKeys = undefined;
    this.lKeys = undefined;
  }

  sortTransformers() { // TODO thinking about the implementation for the transformers sort

  }

  // Getters and setters

  getStep() {
    return this.step;
  }

  setStep(step) {
    this.step = step;
  }

  getTransformerType(transformer) {
    var index = -1;
    for (var i = 0; i < this.transformers.length; i++) {
      for (var property in this.transformers[i]) {
        if (this.transformers[i][property] === transformer[property]) {
          index = i;
          break;
        }
      }
    }
    return this.transformerTypes[index];
  }

  // Misc

  previewFile() {
    var fileInput = document.getElementById('fileInput');
    var fileDisplayArea = document.getElementById('fileDisplayArea');
    var context = this;

    fileInput.addEventListener('change', function(e) {
      var file = (<any>fileInput).files[0];
      var textType = /json.*/;

      if (file.type.match(textType)) {
        var reader = new FileReader();

        reader.onload = function(e) {
          fileDisplayArea.innerText = reader.result;
          context.importReady = true;
          context.oldConfig = reader.result;
        }

        reader.readAsText(file);
      }
      else fileDisplayArea.innerText = "You selected an invalid configuration";

    });
  }



  showSource() {
    $("#sourceArea").fadeIn(1000);
  }

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
