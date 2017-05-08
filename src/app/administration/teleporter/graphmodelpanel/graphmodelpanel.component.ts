import {Component, Input, Output, EventEmitter, ViewChild, OnChanges} from '@angular/core';
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

class GraphModelPanelComponent implements OnChanges {

  @Input() modellingConfig = this.modellingConfig !== 'undefined' ? this.modellingConfig : 'no config from parent.';
  @Output() modellingConfigChange = new EventEmitter();

  @Input() selectedElement = this.selectedElement !== 'undefined' ? this.selectedElement : 'undefined';
  @Output() onSelectedElement = new EventEmitter();

  public searchOptions: Array<Select2OptionData>;
  private selectOptions;
  private value;

  @ViewChild('graph') graphComponent;

  constructor() {

    this.searchOptions = [];
    this.value = "";

    this.selectOptions = {
      multiple: false,
      theme: 'classic',
      closeOnSelect: true,
      maximumSelectionSize: 1,
      placeholder: "Class or Table name"
    }

  }

  ngOnChanges(changes) {

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
  }

  updateSelectedElement(e) {
    this.selectedElement = e;
    this.onSelectedElement.emit(this.selectedElement);
  }

  searchNode(e: any) {
    this.value = e.value;
    this.graphComponent.searchNode(this.value);
  }

}

angular.module('graphmodelpanel.component', []).directive(
  `graph-model-panel`,
  downgradeComponent({component: GraphModelPanelComponent}));


export {GraphModelPanelComponent};
