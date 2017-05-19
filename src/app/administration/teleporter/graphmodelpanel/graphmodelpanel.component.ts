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
  styleUrls: []
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

  @ViewChild('graph') graphComponent;

  constructor() {

    this.dataLoaded = false;
    this.searchOptions = [];
    this.searchOptionsLoaded = false;
    this.searchValue = "";

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

  searchNode(e: any) {
    this.searchValue = e.value;
    this.graphComponent.searchNode(this.searchValue);
  }

}

angular.module('graphmodelpanel.component', []).directive(
  `graph-model-panel`,
  downgradeComponent({component: GraphModelPanelComponent}));


export {GraphModelPanelComponent};
