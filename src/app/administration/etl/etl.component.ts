import * as $ from "jquery"

import {Component, NgZone} from '@angular/core';

import {downgradeComponent} from "@angular/upgrade/static";
import {EtlService} from "../../core/services";
import {AgentService} from "../../core/services/agent.service";
import {ObjectKeysPipe} from "../../core/pipes";
import "../../util/draggable-sortable/jquery-sortable.js";

declare const angular:any;

@Component({
  selector: 'etl',
  templateUrl: "./etl.component.html",
  styleUrls: []
})

class EtlComponent {

  // Prototypes
  private configPrototype;
  private beginPrototype;
  private endPrototype;
  private sourcePrototype;
  private extractorPrototype;
  private transformerPrototype;
  private loaderPrototype;

  // Configuration parts
  private config;
  private source;

  private extractor;

  private currentTransformer;
  private transformers = [];

  private loader;
  private classes;
  private indexes;

  // Types needed for controls
  private extractorType;
  private loaderType;

  // Control booleans
  private advanced;
  private ready;
  private classReady;
  private indexReady;
  private importReady;
  private oldConfigJquery;

  // Execution related variables
  private oldConfig;
  private finalJson;
  private job;
  private jobRunning;

  // Misc
  private step;
  private hints;

  constructor(private agentService: AgentService, private etlService: EtlService, private zone: NgZone, private objectKeys: ObjectKeysPipe) {
    this.objectKeys = objectKeys;

    this.init();

    /*this.agentService.isActive().then(() => {
     this.init();
     }); */ // TODO activate enterprise control
  }

  init() {
    this.configPrototype = {
      config: {
        log: {
          mandatory: false,
          value: "INFO",
          types: ["NONE", "ERROR", "INFO", "DEBUG"]
        },
        maxRetries: {
          mandatory: false,
          value: 10
        },
        parallel: {
          mandatory: false,
          value: false,
        },
        haltOnError: {
          mandatory: false,
          value: true,
        }
      }
    };

    this.beginPrototype = { // TODO add this module. it's an array of blocks
      begin: {

      }
    };

    this.endPrototype = { // TODO add this module. it's an array of blocks
      end: {

      }
    };

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
    };

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
          value: [],
          array: true
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
          value: undefined,
          number: true
        },
        skipTo: {
          mandatory: false,
          value: undefined,
          number: true
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
          value: [],
          array: true
        }
      }

    };

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

      // The 3 possible blocks. It's a special transformer, the implementation is different
      let: {
        name: {
          mandatory: true,
          value: undefined
        },
        value: {
          mandatory: false,
          value: undefined
        },
        expression: {
          mandatory: false,
          value: undefined
        }
      },

      code: {
        language: {
          mandatory: false,
          value: "JavaScript",
          types: ["JavaScript"]
        },
        code: {
          mandatory: true,
          value: undefined
        },
      },

      console: {
        file: {
          mandatory: false,
          value: undefined
        },
        commands: {
          mandatory: false,
          value: undefined,
          array: true
        }
      },

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

    };

    this.loaderPrototype = {
      // Log has no parameters

      orientdb: {
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
          value: 0,
          number: true
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
          value: {
            name: {
              mandatory: true,
              value: undefined
            },
            extends: {
              mandatory: false,
              value:undefined
            },
            clusters: {
              mandatory: false,
              value: 1,
              number: true
            }
          },
          object: true
        },
        indexes: {
          mandatory: false,
          value: {
            name: {
              mandatory: false,
              value: undefined
            },
            class: {
              mandatory: true,
              value: undefined
            },
            type: {
              mandatory: true,
              value: undefined
            },
            fields: {
              mandatory: true,
              value: undefined,
              array: true
            },
            metadata: {
              mandatory: false,
              value: undefined
            }
          },
          object: true
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

    };

    // User support
    this.advanced = false;
    this.ready = false;
    this.classReady = false;
    this.indexReady = false;
    this.importReady = false;
    this.oldConfigJquery = false;
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
      advancedHint: "The ETL module binds all values declared in the config block to the execution context and are accessible to ETL processing.",
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
        name: "Defines the variable name. The ETL process ignores any values with the $ prefix",
        file: "Defines the path to a file containing the commands you want to execute",
        commands: "Defines an array of commands, as strings, to execute in sequence",
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
        cluster: "Defines the cluster in which to store the new record",
        classes: "Defines whether it creates classes, if not defined already in the database",
        indexes: "Defines indexes to use on the ETL process. Before starting, it creates any declared indexes not present in the database. Indexes must have 'type', 'class' and 'fields'",
        useLightweightEdges: "Defines whether it changes the default setting for Lightweight Edges",
        standardElementConstraints: "Defines whether it changes the default setting for TinkerPop Blueprint constraints. Value cannot be null and you cannot use id as a property name",
        name: "Defines the name",
        extends: "Defines the super-class name",
        clusters: "Defines the number of cluster to create under the class",
        metadata: "Defines additional index metadata",
        fields: "Defines an array of fields to index. To specify the field type, use the syntax: field.type, separing the fields with a comma",
        type: "Defines the index type",
        class: "Defines the class name in which to create the index ot the class to use in storing new record"
      }
    }
  }

  // Dynamic creations

  sourceInit() {
    this.step = 2;

    if (this.sourcePrototype.source.value === "jdbc") this.source = null;

    if (this.sourcePrototype.source.value === "local file")
      this.source = {
        file: {
          path: this.sourcePrototype.filePath,
          lock: this.sourcePrototype.fileLock,
          encoding: "UTF-8"
        }
      };

    if (this.sourcePrototype.source.value === "url")
      this.source = {
        http: {
          url: this.sourcePrototype.fileURL,
          method: this.sourcePrototype.URLMethod.value,
          headers: {
            "User-Agent": ""
          }
        }
      };
  }

  extractorInit(type) {
    // Variable creation
    if (type === "row")
      this.extractor = {
        row: {
          multiline: this.extractorPrototype.row.multiline.value,
          linefeed: this.extractorPrototype.row.linefeed.value
        }
      };

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
      };

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
      };

    if (type === "json")
      this.extractor = {
        json: {}
      };

    if (type === "xml")
      this.extractor = {
        xml: {
          rootNode: this.extractorPrototype.xml.rootNode.value,
          tagsAsAttribute: this.extractorPrototype.xml.tagsAsAttribute.value
        }
      };

    this.extractorType = type;
    this.drawExtractorCanvas();
    this.readyForExecution();

    // Jquery
    $("#createExtractor").hide();
    $("#pleaseExtractor").hide();
    $("#panelPlaceholder").hide();
    $("#loaderOptions").hide();
    $("#transformerOptions").hide();
    $("#extractorOptions").show();
  }

  transformerInit(type) { // Csv is deprecated.
    let transformer;

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
      };

    if (type === "merge")
      transformer = {
        merge: {
          joinFieldName: this.transformerPrototype.merge.joinFieldName.value,
          lookup: this.transformerPrototype.merge.lookup.value,
          unresolvedLinkAction: this.transformerPrototype.merge.unresolvedLinkAction.value
        }
      };

    if (type === "vertex")
      transformer = {
        vertex: {
          class: this.transformerPrototype.vertex.class.value,
          skipDuplicates: this.transformerPrototype.vertex.skipDuplicates.value
        }
      };

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
      };

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
      };

    if (type === "flow")
      transformer = {
        flow: {
          if: this.transformerPrototype.flow.if.value,
          operation: this.transformerPrototype.flow.operation.value
        }
      };

    if (type === "log")
      transformer = {
        log: {
          prefix: this.transformerPrototype.log.prefix.value,
          postfix: this.transformerPrototype.log.postfix.value
        }
      };

    // let, code and console are part of the block transformer, they need a different management
    if (type === "let")
      transformer = {
        let: {
          name: this.transformerPrototype.let.name.value,
          value: this.transformerPrototype.let.value.value,
          expression: this.transformerPrototype.let.expression.value
        }
      };

    if (type === "console")
      transformer = {
        console: {
          file: this.transformerPrototype.console.file.value,
          commands: this.transformerPrototype.console.commands.value
        }
      };

    if (type === "code")
      transformer = {
        code: {
          language: this.transformerPrototype.code.language.value,
          code: this.transformerPrototype.code.code.value,
        }
      };

    if (type === "command")
      transformer = {
        command: {
          language: this.transformerPrototype.command.language.value,
          command: this.transformerPrototype.command.command.value
        }
      };

    // Push into the arrays
    this.transformers.push(transformer);
    this.currentTransformer = transformer;
    this.readyForExecution();

    // Canvas
    let index = this.transformers.indexOf(transformer).toString();
    let listEntry = document.createElement('li');
    let canvas = document.createElement('canvas');
    let ctx = canvas.getContext('2d');

    listEntry.id = 'listEntry' + index;

    canvas.width = 300;
    canvas.height = 120;
    canvas.style.width = 200 + "px";
    canvas.style.height = 80 + "px";
    canvas.style.position = "relative";
    canvas.id = index;

    ctx.beginPath();
    ctx.rect(10, 10, 220, 100);
    ctx.fillStyle = '#eeeeee';
    ctx.fill();
    ctx.lineWidth = 4;
    ctx.strokeStyle = '#444444';
    ctx.stroke();
    ctx.font = "bold 24px sans-serif";
    ctx.fillStyle = "#00b386";
    ctx.fillText(type.toUpperCase(),50,50);
    ctx.font = "18px sans-serif";
    ctx.fillStyle = "#444444";
    ctx.fillText("click to configure",50,85);

    let list = document.getElementById("transformerList");
    list.appendChild(listEntry);
    listEntry.appendChild(canvas);

    let tCanvas = $('#' + index);

    tCanvas.on('click', () => {this.selectTransformer(Number(canvas.id))});

    // Jquery
    $("#pleaseTransformer").hide();
    $("#panelPlaceholder").hide();
    $("#loaderOptions").hide();
    $("#extractorOptions").hide();
    $("#transformerOptions").show();
  }

  loaderInit(type) {
    // Variable creation
    if (type === "log")
      this.loader = {
        log: {}
      };

    if (type === "orientdb") {
      this.loader = {
        orientdb: {
          dbURL: this.loaderPrototype.orientdb.dbURL.value,
          dbUser: this.loaderPrototype.orientdb.dbUser.value,
          dbPassword: this.loaderPrototype.orientdb.dbPassword.value,
          serverUser: this.loaderPrototype.orientdb.serverUser.value,
          serverPassword: this.loaderPrototype.orientdb.serverPassword.value,
          dbAutoCreate: this.loaderPrototype.orientdb.dbAutoCreate.value,
          dbAutoCreateProperties: this.loaderPrototype.orientdb.dbAutoCreateProperties.value,
          dbAutoDropIfExists: this.loaderPrototype.orientdb.dbAutoDropIfExists.value,
          tx: this.loaderPrototype.orientdb.tx.value,
          txUseLog: this.loaderPrototype.orientdb.txUseLog.value,
          wal: this.loaderPrototype.orientdb.wal.value,
          batchCommit: this.loaderPrototype.orientdb.batchCommit.value,
          dbType: this.loaderPrototype.orientdb.dbType.value,
          class: this.loaderPrototype.orientdb.class.value,
          cluster: this.loaderPrototype.orientdb.cluster.value,
          classes: [], // empty arrays. The default value of the related object is pushed and modified when the user click on the add button
          indexes: [],
          useLightweightEdges: this.loaderPrototype.orientdb.useLightweightEdges.value,
          standardElementConstraints: this.loaderPrototype.orientdb.standardElementConstraints.value
        }
      };

      this.classes = {
        name: this.loaderPrototype.orientdb.classes.value.name.value,
        extends: this.loaderPrototype.orientdb.classes.value.extends.value,
        clusters: this.loaderPrototype.orientdb.classes.value.clusters.value
      };

      this.indexes = {
        name: this.loaderPrototype.orientdb.indexes.value.name.value,
        class: this.loaderPrototype.orientdb.indexes.value.class.value,
        type: this.loaderPrototype.orientdb.indexes.value.type.value,
        fields: this.loaderPrototype.orientdb.indexes.value.fields.value,
        metadata: this.loaderPrototype.orientdb.indexes.value.metadata.value
      };
    }

    this.loaderType = type;
    this.drawLoaderCanvas();
    this.readyForExecution();

    // Jquery
    $("#pleaseLoader").hide();
    $("#createLoader").hide();
    $("#panelPlaceholder").hide();
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
    $("#eCanvas").fadeOut(1000);

  }

  deleteTransformer() {
    let index = this.transformers.indexOf(this.currentTransformer);
    if (index > -1) {
      this.transformers.splice(index, 1);
    }
    this.currentTransformer = undefined;

    // remove the canvas and update the ids to match the array indexes
    $("#" + index).remove();

    let canvas = $("#transformerList").find("canvas");
    for(let i = index; i < this.transformers.length; i++) {
      (<any>canvas[i]).setAttribute("id", i);
    }

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
    $("#lCanvas").fadeOut(1000);
  }

  oldConfigInit(oldConfig, direct = 0) {
    let etl = JSON.parse(oldConfig);

    this.extractorType = Object.getOwnPropertyNames(etl.extractor)[0];
    this.currentTransformer = etl.transformers[etl.transformers.length -1];
    this.loaderType = Object.getOwnPropertyNames(etl.loader)[0];

    if(etl.config) this.config = etl.config;
    this.extractor = etl.extractor;
    this.transformers = etl.transformers;
    this.reverseBlockFix();
    this.loader = etl.loader; // TODO manage objects inside
    // initialize the inner objects
    if(this.loaderType === 'orientdb') {
      this.classes = {
        name: this.loaderPrototype.orientdb.classes.value.name.value,
        extends: this.loaderPrototype.orientdb.classes.value.extends.value,
        clusters: this.loaderPrototype.orientdb.classes.value.clusters.value
      };
      this.indexes = {
        name: this.loaderPrototype.orientdb.indexes.value.name.value,
        class: this.loaderPrototype.orientdb.indexes.value.class.value,
        type: this.loaderPrototype.orientdb.indexes.value.type.value,
        fields: this.loaderPrototype.orientdb.indexes.value.fields.value,
        metadata: this.loaderPrototype.orientdb.indexes.value.metadata.value
      };
    }


    // Skip step 1 if the old configuration uses a jdbc source
    if(etl.source) {
      this.source = etl.source;
      this.sourcePrototype.source.value = Object.getOwnPropertyNames(etl.source)[0];
      if(direct) {
        this.launch();
        return;
      }
      this.setStep(1);
    }
    else {
      this.sourcePrototype.source.value = "jdbc";
      if(direct) {
        this.launch();
        return;
      }
      this.setStep(2);
    }

    // eventually activate the run button, the jquery part is triggered by the viewchecked event
    this.readyForExecution();
    this.oldConfigJquery = true;
  }

  drawExtractorCanvas() {
    let canvas = <HTMLCanvasElement> document.getElementById("eCanvas");
    canvas.width = 300;
    canvas.height = 120;
    canvas.style.width = 200 + "px";
    canvas.style.height = 80 + "px";
    canvas.style.position = "relative";
    canvas.style.top = "70px";
    canvas.style.left = "20px";

    let ctx = canvas.getContext("2d");
    ctx.beginPath();
    ctx.rect(10, 10, 220, 100);
    ctx.fillStyle = '#eeeeee';
    ctx.fill();
    ctx.lineWidth = 4;
    ctx.strokeStyle = '#444444';
    ctx.stroke();
    ctx.font = "bold 24px sans-serif";
    ctx.fillStyle = "#ff9966";
    ctx.fillText(this.extractorType.toUpperCase(),50,50);
    ctx.font = "18px sans-serif";
    ctx.fillStyle = "#444444";
    ctx.fillText("click to configure",50,85);

    (<any>$("#eCanvas")).slideDown(1000); // Canvas animation
  }

  drawTransformerCanvas() { // TODO wrong interaction and they are unsortable with any new created transformer
    for(let i = 0; i < this.transformers.length; i++) {
      let listEntry = document.createElement('li');
      let canvas = document.createElement('canvas');
      let ctx = canvas.getContext('2d');

      listEntry.id = 'listEntry' + i;

      canvas.width = 300;
      canvas.height = 120;
      canvas.style.width = 200 + "px";
      canvas.style.height = 80 + "px";
      canvas.style.position = "relative";
      canvas.id = String(i);

      ctx.beginPath();
      ctx.rect(10, 10, 220, 100);
      ctx.fillStyle = '#eeeeee';
      ctx.fill();
      ctx.lineWidth = 4;
      ctx.strokeStyle = '#444444';
      ctx.stroke();
      ctx.font = "bold 24px sans-serif";
      ctx.fillStyle = "#00b386";
      ctx.fillText(this.getTransformerType(this.transformers[i]).toUpperCase(), 50, 50);
      ctx.font = "18px sans-serif";
      ctx.fillStyle = "#444444";
      ctx.fillText("click to configure", 50, 85);

      let list = document.getElementById("transformerList");
      list.appendChild(listEntry);
      listEntry.appendChild(canvas);


      let tCanvas = $('#' + i);

      tCanvas.on('click', () => {
        this.selectTransformer(Number(canvas.id))
      });
    }
  }

  drawLoaderCanvas() {
    let canvas = <HTMLCanvasElement> document.getElementById("lCanvas");
    canvas.width = 300;
    canvas.height = 120;
    canvas.style.width = 200 + "px";
    canvas.style.height = 80 + "px";
    canvas.style.position = "relative";
    canvas.style.top = "70px";
    canvas.style.left = "20px";

    let ctx = canvas.getContext("2d");
    ctx.beginPath();
    ctx.rect(10, 10, 220, 100);
    ctx.fillStyle = '#eeeeee';
    ctx.fill();
    ctx.lineWidth = 4;
    ctx.strokeStyle = '#444444';
    ctx.stroke();
    ctx.font = "bold 24px sans-serif";
    ctx.fillStyle = "#bf80ff";
    ctx.fillText(this.loaderType.toUpperCase(),50,50);
    ctx.font = "18px sans-serif";
    ctx.fillStyle = "#444444";
    ctx.fillText("click to configure",50,85);

    let lCanvas = $('#lCanvas');
    (<any>lCanvas).slideDown(1000); // Canvas animation

  }

  // Core Functions

  readyForExecution() {

    // If one module for every type exists (source excluded)
    if (this.extractor && this.transformers.length > 0 && this.loader) {
      // Controls, for every property of every module, if it's mandatory and has a value
      // Extractor control
      for(let property in this.extractorPrototype[this.extractorType]) {
        if(this.extractorPrototype[this.extractorType].hasOwnProperty(property)) {
          if(this.extractorPrototype[this.extractorType][property]["mandatory"] && !this.extractor[this.extractorType][property]) {
            this.ready = false;
            return;
          }
        }
      }
      // Transformers control
      for(let i = 0; i < this.transformers.length; i++) {
        let type = this.getTransformerType(this.transformers[i]);
        for(let property in this.transformerPrototype[type]) {
          if(this.transformerPrototype[type].hasOwnProperty(property)) {
            if(this.transformerPrototype[type][property]["mandatory"] && !this.transformers[i][type][property]) {
              this.ready = false;
              return;
            }
          }
        }
      }
      // Loader control
      for(let property in this.loaderPrototype[this.loaderType]) {
        if(this.loaderPrototype[this.loaderType].hasOwnProperty(property)) {
          if(this.loaderPrototype[this.loaderType][property]["mandatory"] && !this.loader[this.loaderType][property]) {
            this.ready = false;
            return;
          }
        }
      }
      this.ready = true;
    }
  }

  subReadyForExecution() {

    // Controls that the objects in OrientDB loader are correctly created
    for(let property in this.classes) {
      if(this.classes.hasOwnProperty(property))
        if(this.loaderPrototype.orientdb.classes["value"][property]["mandatory"] && !this.classes[property]) {
          this.classReady = false;
          break;
        }
      this.classReady = true;
    }

    for(let property in this.indexes) {
      if(this.indexes.hasOwnProperty(property)) {
        if(property === "fields" && this.indexes.fields)
          if(this.indexes.fields.length == 0) {
            this.indexReady = false;
            break;
          }
        if(this.loaderPrototype.orientdb.indexes["value"][property]["mandatory"] && !this.indexes[property]) {
          this.indexReady = false;
          break;
        }
      }
      this.indexReady = true;
    }
  }

  launch() {
    let etl = {}; // optional source control. Is that needed?

    this.sortTransformers(); // Sorts transformer starting from the indexes
    this.blockFix(); // Fixes the different behavior of block transformers

    if(this.source && this.config)
      etl = {
        config: this.config,
        source: this.source,
        extractor: this.extractor,
        transformers: this.transformers,
        loader: this.loader
      };

    if(!this.source && this.config)
      etl = {
        config: this.config,
        extractor: this.extractor,
        transformers: this.transformers,
        loader: this.loader
      };

    if(this.source && !this.config)
      etl = {
        source: this.source,
        extractor: this.extractor,
        transformers: this.transformers,
        loader: this.loader
      };
    if(!this.source && !this.config)
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
    if(this.jobRunning) {
      this.etlService.status().then((data) => {
        if(data.jobs.length > 0) {
          this.job = data.jobs[0];
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

  reset() {
    // useful assignations to reset the configuration
    this.transformers = [];
    this.currentTransformer = undefined;
    this.config = undefined;
    this.extractor = undefined;
    this.loader = undefined;
    this.source = undefined;
    this.sourcePrototype.source.value = undefined;
    this.extractorType = undefined;
    this.loaderType = undefined;
    this.advanced = false;
    this.ready = false;
    this.classReady = false;
    this.indexReady = false;
    this.importReady = false;
    this.oldConfigJquery = false;
    this.oldConfig = undefined;
  }

  blockFix() {
    let tmp = [];
    let tmpBlock;
    for(let i = 0; i < this.transformers.length; i++) {
      if(this.transformers[i]['let'] || this.transformers[i]['console']) {
        // even code transformer could be included in if statement
        tmpBlock = {block: this.transformers[i]};
        tmp.push(tmpBlock);
      }
      else tmp.push(this.transformers[i]);
    }
    this.transformers = tmp;
  }

  reverseBlockFix() {
    let tmp = [];
    let tmpBlock;
    for(let i = 0; i < this.transformers.length; i++) {
      if(this.transformers[i]['block']) {
        tmpBlock = this.transformers[i]['block'];
        tmp.push(tmpBlock);
      }
      else tmp.push(this.transformers[i]);
    }
    this.transformers = tmp;
  }

  sortTransformers() {
    let IDs = [];
    let tmp = [];
    $("#transformerList").find("canvas").each(function(){ IDs.push(this.id); });
    if(IDs.length == 0) return; // useful for direct run feature
    for(let i = 0; i < this.transformers.length; i++) {
      tmp.push(this.transformers[Number(IDs[i])]);
    }
    this.transformers = tmp;
  }

  loaderAddTo(type) {
    if(type === "classes") {
      this.loader.orientdb.classes.push(this.classes);
      this.classes = {
        name: this.loaderPrototype.orientdb.classes.value.name.value,
        extends: this.loaderPrototype.orientdb.classes.value.extends.value,
        clusters: this.loaderPrototype.orientdb.classes.value.clusters.value
      };
      this.classReady = false;
    }
    if(type === "indexes") {
      this.loader.orientdb.indexes.push(this.indexes);
      this.indexes = {
        name: this.loaderPrototype.orientdb.indexes.value.name.value,
        class: this.loaderPrototype.orientdb.indexes.value.class.value,
        type: this.loaderPrototype.orientdb.indexes.value.type.value,
        fields: this.loaderPrototype.orientdb.indexes.value.fields.value,
        metadata: this.loaderPrototype.orientdb.indexes.value.metadata.value
      };
      this.indexReady = false;
    }
  }

  loaderEmpty(type) {
    if(type === "classes") this.loader.orientdb.classes = [];
    if(type === "indexes") this.loader.orientdb.indexes = [];
  }

  // Getters and setters

  getStep() {
    return this.step;
  }

  setStep(step) {
    this.step = step;
  }

  getTransformerType(transformer) {
    if(!transformer) return undefined;

    // for every transformer, and every property of every transformer (just the type), return the property (i.e. the type) if it's equal
    for (let i = 0; i < this.transformers.length; i++) {
      for (let property in this.transformers[i]) {
        if (this.transformers[i].hasOwnProperty(property)) {
          if (this.transformers[i][property] === transformer[property]) return property;
        }
      }
    }
    return undefined;
  }

  // Misc

  separeCaps(string) {
    string = string.split(/(?=[A-Z])/).join(" ");
    return string;
  }

  previewFile() {
    let fileInput = document.getElementById('fileInput');
    let fileDisplayArea = document.getElementById('fileDisplayArea');
    let context = this;

    fileInput.addEventListener('change', function(e) {
      let file = (<any>fileInput).files[0];
      let textType = /json.*/;

      if (file.type.match(textType)) {
        let reader = new FileReader();

        reader.onload = function() {
          fileDisplayArea.innerText = reader.result;
          context.importReady = true;
          context.oldConfig = reader.result;
        };

        reader.readAsText(file);
      }
      else fileDisplayArea.innerText = "You selected an invalid configuration";

    });
  }


  addAdvanced() {
    this.advanced = true;

    this.config = {
      log: this.configPrototype.config.log.value,
      maxRetries: this.configPrototype.config.maxRetries.value,
      haltOnError: this.configPrototype.config.haltOnError.value,
      parallel: this.configPrototype.config.parallel.value
    }
  }

  showSource() {
    $("#sourceArea").fadeIn(1000);
  }

  selectExtractor() {
    $("#extractorOptions").slideDown(800);
    $("#transformerOptions").slideUp(800);
    $("#loaderOptions").slideUp(800);
    $("#panelPlaceholder").slideUp(800);
  }

  selectTransformer(index) {
    this.currentTransformer = this.transformers[index];

    $("#extractorOptions").slideUp(800);
    $("#transformerOptions").slideDown(800);
    $("#loaderOptions").slideUp(800);
    $("#panelPlaceholder").slideUp(800);
  }

  selectLoader() {
    $("#extractorOptions").slideUp(800);
    $("#transformerOptions").slideUp(800);
    $("#loaderOptions").slideDown(800);
    $("#panelPlaceholder").slideUp(800);
  }

  scrollLogAreaDown() {
    let logArea = $("#logArea");
    logArea.scrollTop(9999999);
  }

  ngAfterViewChecked() {
    this.enablePopovers();

    // Activate sortable and custom animations
    if(this.step == 2) {
      let adjustment;
      (<any>$("#transformerList")).sortable({
        group: 'transformerList',
        pullPlaceholder: false,
        // animation on drop
        onDrop: function ($item, container, _super) {
          let $clonedItem = $('<li/>').css({height: 0});
          $item.before($clonedItem);
          $clonedItem.animate({'height': $item.height()});
          $item.animate($clonedItem.position(), function () {
            $clonedItem.detach();
            _super($item, container);
          });
        },
        // set $item relative to cursor position
        onDragStart: function ($item, container, _super) {
          let offset = $item.offset(),
            pointer = container.rootGroup.pointer;

          adjustment = {
            left: pointer.left - offset.left,
            top: pointer.top - offset.top
          };
          _super($item, container);
        },
        onDrag: function ($item, position) {
          $item.css({
            left: position.left - adjustment.left,
            top: position.top - adjustment.top
          });
        }
      });
    }
    // Execute the Jquery part just one time
    if(this.oldConfig && this.step == 2 && this.oldConfigJquery) {
      // Jquery
      $(document).ready(function () {
        $("#pleaseExtractor").hide();
        $("#pleaseTransformer").hide();
        $("#pleaseLoader").hide();
        $("#createExtractor").hide();
        $("#createLoader").hide();
        $("#panelPlaceholder").show();
      });

      this.drawExtractorCanvas();
      this.drawTransformerCanvas();
      this.drawLoaderCanvas();
      this.oldConfigJquery = false;
    }
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
