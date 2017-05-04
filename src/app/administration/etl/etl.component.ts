import * as $ from "jquery"
import * as jqueryUI from "jquery-ui";

import "../../util/diagram-editor/jquery.flowchart.min.js";
import "../../util/diagram-editor/jquery.flowchart.min.css";
import "../../util/diagram-editor/flowchart.jquery.json";
import "../../util/diagram-editor/package.json";

import {Component} from '@angular/core';

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
  private sourcePrototype;
  private extractorPrototype;
  private transformerPrototype;
  private loaderPrototype;
  private source; // Source variable
  private extractor; // Extractor variable
  private transformers = []; // Array containing every transformer
  private loader; // Loader variable

  private finalJson; // The final json, it will be passed to the launch function

  private step;
  private hints;
  private ready = false;


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
        types: ["jdbc", "local file", "url"]
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
        multiline: true,
        linefeed: "\r\n",
      },

      csv: {
        separator: ",",
        columnsOnFirstLine: true,
        columns: undefined,
        nullValue: "NULL",
        dateFormat: "yyyy-MM-dd",
        dateTimeFormat: "yyyy-MM-dd HH:mm",
        quote: '"',
        skipFrom: undefined,
        skipTo: undefined,
        ignoreEmptyLines: false,
        ignoreMissingColumns: false,
        predefinedFormat: {
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
        queryCount: undefined
      },

      // Json has no parameters

      xml: {
        rootNode: undefined,
        tagsAsAttribute: []
      }

    }

    this.transformerPrototype = {
      customLabel: undefined,

      field: {
        fieldName: undefined,
        expression: {
          mandatory: true,
          value: undefined
        },
        value: undefined,
        operation: {
          value: "SET",
          types: ["SET", "REMOVE"]
        },
        save: false
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
          value: "NOTHING",
          types: ["NOTHING", "WARNING", "ERROR", "HALT", "SKIP"]
        }
      },

      vertex: {
        class: "V",
        skipDuplicates: false
      },

      edge: {
        joinFieldName: {
          mandatory: true,
          value: undefined
        },
        direction: {
          value: "out",
          types: ["in", "out"]
        },
        class: "E",
        lookup: {
          mandatory: true,
          value: undefined
        },
        targetVertexFields: undefined,
        edgeFields: undefined,
        skipDuplicates: false,
        unresolvedLinkAction: {
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
          value: undefined,
          types: ["skip", "halt"]
        },
      },

      code: { // TODO probably unadoptable
        language: "JavaScript",
        code: {
          mandatory: true,
          value: undefined
        },
      },

      link: {
        joinFieldName: undefined,
        joinValue: undefined,
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
          value: "NOTHING",
          types: ["NOTHING", "WARNING", "ERROR", "HALT", "SKIP"]
        },
      },

      log: {
        prefix: undefined,
        postfix: undefined
      },

      // Block has no parameters

      command: {
        language: {
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
        dbType: {
          value: "document",
          types: ["document", "graph"]
        },
        class: undefined,
        cluster: undefined,
        classes: undefined,
        indexes: undefined,
        useLightweightEdges: false,
        standardElementConstraints: true
      }

    }

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
    $("#createExtractor").hide();
    $("#pleaseExtractor").hide();

    $("#panelPlaceholder").hide(); // Shows the options for the extractor, hides the placeholder
    $("#loaderOptions").hide();
    $("#transformerOptions").hide();
    $("#extractorOptions").show();

    // Variable creation
    if(type === "row") {
      this.extractor = {
        row: {
          multiline: this.extractorPrototype.row.multiline,
          linefeed: this.extractorPrototype.row.linefeed
        }
      }
    }

    if(type === "csv") {
      this.extractor = {
        csv: {
          separator: this.extractorPrototype.csv.separator,
          columnsOnFirstLine: this.extractorPrototype.csv.columnsOnFirstLine,
          columns: this.extractorPrototype.csv.columns,
          nullValue: this.extractorPrototype.csv.nullValue,
          dateFormat: this.extractorPrototype.csv.dateFormat,
          dateTimeFormat: this.extractorPrototype.csv.dateTimeFormat,
          quote: this.extractorPrototype.csv.quote,
          skipFrom: this.extractorPrototype.csv.skipFrom,
          skipTo: this.extractorPrototype.csv.skipTo,
          ignoreEmptyLines: this.extractorPrototype.csv.ignoreEmptyLines,
          ignoreMissingColumns: this.extractorPrototype.csv.ignoreMissingColumns,
          predefinedFormat: this.extractorPrototype.csv.predefinedFormat.value
        }
      }
    }

    if(type === "jdbc") {
      this.extractor = {
        jdbc: {
          driver: this.extractorPrototype.jdbc.driver.value,
          url: this.extractorPrototype.jdbc.url.value,
          userName: this.extractorPrototype.jdbc.userName.value,
          userPassword: this.extractorPrototype.jdbc.userPassword.value,
          query: this.extractorPrototype.jdbc.query.value,
          queryCount: this.extractorPrototype.jdbc.queryCount
        }
      }
    }

    if(type === "json") {
      this.extractor = {
        json: {}
      }
    }

    if(type === "xml") {
      this.extractor = {
        xml: {
          rootNode: this.extractorPrototype.xml.rootNode,
          tagsAsAttribute: this.extractorPrototype.xml.tagsAsAttribute
        }
      }
    }

    // Flowchart
    $(document).ready(function() {
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
    });

    this.readyForExecution();
  }

  transformerInit(type) { // Block and Code aren't active atm. Csv is deprecated.
    $("#pleaseTransformer").hide();

    $("#panelPlaceholder").hide(); // Shows the options for the extractor, hides the placeholder
    $("#loaderOptions").hide();
    $("#extractorOptions").hide();
    $("#transformerOptions").show();

    var transformer;
    // TODO: use the custom label in the flowchart, show the options for the created transformer

    // Variable creation
    if(type === "field") {
      transformer = {
        field: {
          fieldName: this.transformerPrototype.field.fieldName,
          expression: this.transformerPrototype.field.expression.value,
          value: this.transformerPrototype.field.value,
          operation: this.transformerPrototype.field.operation.value,
          save: this.transformerPrototype.field.save
        }
      }
    }

    if(type === "merge") {
      transformer = {
        merge: {
          joinFieldName: this.transformerPrototype.merge.joinFieldName.value,
          lookup: this.transformerPrototype.merge.lookup.value,
          unresolvedLinkAction: this.transformerPrototype.merge.unresolvedLinkAction.value
        }
      }
    }

    if(type === "vertex") {
      transformer = {
        vertex: {
          class: this.transformerPrototype.vertex.class,
          skipDuplicates: this.transformerPrototype.vertex.skipDuplicates
        }
      }
    }

    if(type === "code") {
      transformer = {
        code: {
          language: this.transformerPrototype.code.language,
          code: this.transformerPrototype.code.code,
        }
      }
    }

    if(type === "link") {
      transformer = {
        link: {
          joinFieldName: this.transformerPrototype.link.joinFieldName,
          joinValue: this.transformerPrototype.link.joinValue,
          linkFieldName: this.transformerPrototype.link.linkFieldName.value,
          linkFieldType: this.transformerPrototype.link.linkFieldType.value,
          lookup: this.transformerPrototype.link.lookup.value,
          unresolvedLinkAction: this.transformerPrototype.link.unresolvedLinkAction.value
        }
      }
    }

    if(type === "edge") {
      transformer = {
        edge: {
          joinFieldName: this.transformerPrototype.edge.joinFieldName.value,
          direction: this.transformerPrototype.edge.direction.value,
          class: this.transformerPrototype.edge.class,
          lookup: this.transformerPrototype.edge.lookup.value,
          targetVertexFields: this.transformerPrototype.edge.targetVertexFields,
          edgeFields: this.transformerPrototype.edge.edgeFields,
          skipDuplicates: this.transformerPrototype.edge.skipDuplicates,
          unresolvedLinkAction: this.transformerPrototype.edge.unresolvedLinkAction.value
        }
      }
    }

    if(type === "flow") {
      transformer = {
        flow: {
          if: this.transformerPrototype.flow.if.value,
          operation: this.transformerPrototype.flow.operation.value
        }
      }
    }

    if(type === "log") {
      transformer = {
        log: {
          prefix: this.transformerPrototype.log.prefix,
          postfix: this.transformerPrototype.log.postfix
        }
      }
    }

    if(type === "block") {
      transformer = {
        block: {

        }
      }
    }

    if(type === "command") {
      transformer = {
        command: {
          language: this.transformerPrototype.command.language.value,
          command: this.transformerPrototype.command.command.value
        }
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

      (<any>$('#transformerSpace')).flowchart({
        data: dataTransformer
      });
    });

    // Push into the array, return the new transformer
    this.transformers.push(transformer);
    this.readyForExecution();
    return transformer; // TODO maybe useless?

  }

  loaderInit(type) {
    $("#pleaseLoader").hide();
    $("#createLoader").hide();

    $("#panelPlaceholder").hide(); // Shows the options for the extractor, hides the placeholder
    $("#extractorOptions").hide();
    $("#transformerOptions").hide();
    $("#loaderOptions").show();

    // Variable creation
    if(type === "log")
      this.loader = {
        log: {}
      }


    if(type === "OrientDB")
      this.loader = {
        orientdb: {
          dbURL: this.loaderPrototype.orientDb.dbURL.value,
          dbUser: this.loaderPrototype.orientDb.dbUser,
          dbPassword: this.loaderPrototype.orientDb.dbPassword,
          serverUser: this.loaderPrototype.orientDb.serverUser,
          serverPassword: this.loaderPrototype.orientDb.serverPassword,
          dbAutoCreate: this.loaderPrototype.orientDb.dbAutoCreate,
          dbAutoCreateProperties: this.loaderPrototype.orientDb.dbAutoCreateProperties,
          dbAutoDropIfExists: this.loaderPrototype.orientDb.dbAutoDropIfExists,
          tx: this.loaderPrototype.orientDb.tx,
          txUseLog: this.loaderPrototype.orientDb.txUseLog,
          wal: this.loaderPrototype.orientDb.wal,
          batchCommit: this.loaderPrototype.orientDb.batchCommit,
          dbType: this.loaderPrototype.orientDb.dbType.value,
          class: this.loaderPrototype.orientDb.class,
          cluster: this.loaderPrototype.orientDb.cluster,
          classes: this.loaderPrototype.orientDb.classes,
          indexes: this.loaderPrototype.orientDb.indexes,
          useLightweightEdges: this.loaderPrototype.orientDb.useLightweightEdges,
          standardElementConstraints: this.loaderPrototype.orientDb.standardElementConstraints

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

      (<any>$('#loaderSpace')).flowchart({
        data: dataLoader
      });
    });

    this.readyForExecution();
  }

  deleteExtractor() {
    this.extractor = undefined;

    // Jquery hide/show
    $("#createExtractor").show();
    $("#extractorOptions").hide();
    $("#panelPlaceholder").show();
  }

  deleteTransformer(name) { // TODO probably not working
    var v = this.transformers.indexOf(name);
    this.transformers[v] = undefined;

    // Jquery hide/show
    $("#transformerOptions").hide();
    $("#panelPlaceholder").show();
  }

  deleteLoader() {
    this.loader = undefined;

    // Jquery hide/show
    $("#createLoader").show();
    $("#loaderOptions").hide();
    $("#panelPlaceholder").show();
  }


  // Core Functions

  readyForExecution() {
    if(this.extractor)
      this.ready = true;
    else
      this.ready = false;
  }

  launch() {
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
      title: 'About this parameter',
      placement: 'right',
      trigger: 'hover'
    });
  }
}

angular.module('etl.component', []).directive(
  `etl`,
  downgradeComponent({component: EtlComponent}));


export {EtlComponent};
