import {Component, Input, Output, EventEmitter, ViewChild, OnInit, OnChanges} from '@angular/core';
import {Select2OptionData} from 'ng2-select2';

import * as $ from "jquery"

import {downgradeComponent} from '@angular/upgrade/static';
import {TeleporterService} from '../../../core/services';
import {NotificationService} from "../../../core/services/notification.service";

declare var angular:any

@Component({
  selector: 'graph-model-panel',
  templateUrl: "./graphmodelpanel.component.html",
  styles: [
    '.actions-dropdown-item:hover {cursor: pointer}'
  ]
})

class GraphModelPanelComponent implements OnInit, OnChanges {

  @Input() modellingConfig = this.modellingConfig !== 'undefined' ? this.modellingConfig : 'no config from parent.';
  @Output() modellingConfigChange = new EventEmitter();

  @Input() selectedElement = this.selectedElement !== 'undefined' ? this.selectedElement : 'undefined';
  @Output() onSelectedElement = new EventEmitter();

  @Input() onMigrationConfigFetched;

  private dataLoaded:boolean;

  public searchOptions: Array<Select2OptionData>;
  private searchOptionsLoaded;
  private selectOptions;
  private searchValue;
  private searchAllowed;

  private legendShown = false;

  @ViewChild('graph') graphComponent;

  constructor() {

    this.dataLoaded = false;
    this.searchOptions = [];
    this.searchOptionsLoaded = false;
    this.searchValue = "";
    this.searchAllowed = true;

    this.selectOptions = {
      multiple: false,
      theme: 'classic',
      closeOnSelect: true,
      placeholder: "Insert Class or Table name"
    }

  }

  ngOnChanges(changes) {
    if(changes.modellingConfig) {
      if (changes.modellingConfig.currentValue && this.searchOptions.length === 0) {
        this.prepareSearchOptions();
      }
    }
  }

  ngOnInit() {

    // if modellingConfig is undefined the migration config is not fetched yet, thus we can subscribe to the event emitter and wait for completion
    if(!this.modellingConfig) {
      this.onMigrationConfigFetched.subscribe(() => {
        this.dataLoaded = true;
      })
    }
    else {
      // migration config is already fetched, thus we don't need to subscribe to the event emitter
      this.dataLoaded = true;
    }
  }

  redraw() {
    this.graphComponent.redraw();
  }

  prepareSearchOptions() {

    // collect all the node names for search auto-complete
    for (var i = 0; i < this.modellingConfig.vertices.length; i++) {
      var currOption = {
        id: this.modellingConfig.vertices[i].name,
        text: this.modellingConfig.vertices[i].name + " [Vertex-Class]"
      }
      this.searchOptions.push(currOption);

      for(var j=0; j<this.modellingConfig.vertices[i].mapping.sourceTables.length; j++) {

        currOption = {
          id: this.modellingConfig.vertices[i].mapping.sourceTables[j].tableName,
          text: this.modellingConfig.vertices[i].mapping.sourceTables[j].tableName + " [Table]"
        }
        this.searchOptions.push(currOption);
      }
    }
    this.searchOptions = this.searchOptions.sort(function (a, b) {
      return a.id.localeCompare(b.id);
    });
    this.searchOptionsLoaded = true;
    console.log(this.searchOptions);
  }

  updateSelectedElement(e) {
    this.selectedElement = e;
    this.onSelectedElement.emit(this.selectedElement);
  }

  renameElementInGraph(event) {
    var oldClassName = event.oldClassName;
    var newClassName = event.newClassName;
    var classType = event.classType;

    this.graphComponent.renameElementInGraph(oldClassName, newClassName, classType);

    if(classType === 'vertexClass') {
      // updating the searchOptions (used a new variable so the child component will refresh as a new referenced object will be detected)
      var newSearchOptions = JSON.parse(JSON.stringify(this.searchOptions));

      // updating searchOptions
      for (var i = 0; newSearchOptions.length; i++) {
        if (newSearchOptions[i].text === oldClassName + " [Vertex-Class]") {
          newSearchOptions[i].id = newClassName;
          newSearchOptions[i].text = newClassName + " [Vertex-Class]";
          break;
        }
      }

      this.searchAllowed = false;   // to avoid a new search during the select2 child component reloading
      this.searchOptions = newSearchOptions;    // new reference, select2 child component should reload!
      this.searchValue = newClassName;
      setTimeout(() => {
        this.searchAllowed = true;
      }, 500);
    }

  }

  searchNodeOnEvent(e: any) {
    if(this.searchAllowed) {
      this.searchValue = e.value;
      this.searchNode();
    }
  }

  searchNodeOnButtonClick() {
    if(this.searchValue) {
      this.searchNode();
    }
    else {
      alert("Please select a value first!")
    }
  }

  searchNode() {
    if (this.searchValue) {
      if (this.selectedElement) {    // check to avoid double selection due to double event launching
        if (this.selectedElement.name !== this.searchValue) {
          this.graphComponent.searchNode(this.searchValue);
        }
      }
      else {
        this.graphComponent.searchNode(this.searchValue);
      }
    }
  }

  showLegend() {
    this.legendShown = true;
    (<any>$('.graph-legend')).fadeTo(0, 1);
  }

  hideLegend() {
    this.legendShown = false;
    (<any>$('.graph-legend')).fadeTo(0, 0);
  }

  startEdgeCreation() {
    this.graphComponent.startEdgeCreation();
  }

}

angular.module('graphmodelpanel.component', []).directive(
  `graph-model-panel`,
  downgradeComponent({component: GraphModelPanelComponent}));


export {GraphModelPanelComponent};
