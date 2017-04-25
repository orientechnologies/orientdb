import {Component} from '@angular/core';


import * as $ from "jquery"
import {downgradeComponent} from '@angular/upgrade/static';
import {AgentService} from "../../core/services/agent.service";

declare var angular:any

@Component({
  selector: 'etl',
  templateUrl: "./etl.component.html",
  styleUrls: []
})

class EtlComponent {
  private configParams;
  private jsonPrototypes;

  private sourceJson;

  private sourceTypes;
  private extractorTypes;
  private transformerTypes;
  private loaderTypes;

  private URLMethods;

  private step;
  private hints;

  constructor(private agentService: AgentService){

    this.init();

    /*this.agentService.isActive().then(() => {
      this.init();
    }); controls if enterprise*/
  }

  init() {

    // Params to build the partial jsons
    this.configParams = {
      source: "",
      fileURL: "",
      URLMethod: "GET",
      filePath: "",
      fileLock: false,
      extractorType: "",
      transformerType: "",
      customLabel: "",
      loaderType: ""

    }

    // Types for sources, extractors, transformers and loaders
    this.sourceTypes = ["jdbc", "local file", "url"];
    this.extractorTypes = ["csv", "jdbc", "json", "row", "xml"];
    this.transformerTypes = ["block", "code", "command", "csv", "edge", "flow", "field", "link", "log", "merge", "vertex"];
    this.loaderTypes = ["debug", "orientdb"];

    // Specific types
    this.URLMethods = ["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "TRACE"];

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

    // Json prototypes, contains partial jsons
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

  extractorInit() {

  }

  transformerInit() {

  }

  loaderInit() {

  }

  prepareTransformerJson() {

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
