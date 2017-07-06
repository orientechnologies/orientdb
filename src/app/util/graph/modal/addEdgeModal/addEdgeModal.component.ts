import {Component, ViewChild, Input} from '@angular/core';
import {ModalComponent} from 'ng2-bs3-modal/ng2-bs3-modal';
import {Select2OptionData} from 'ng2-select2';

import {downgradeComponent} from '@angular/upgrade/static';
import edge = require("selenium-webdriver/edge");

declare var angular:any

@Component({
  selector: 'add-edge-modal',
  templateUrl: "./addEdgeModal.component.html",
  styleUrls: []
})

class AddEdgeModal {

  private tmpEdgeName;
  private tmpEdgeDefinition;
  private tmpEdgeMapping;
  private fromVertex;
  private toVertex;

  private activeFormNewEdge = "1";
  private directions;

  @Input() graphComponent;

  /**
   * Modals
   */
  @ViewChild('addEdgeModal') addEdgeModal: ModalComponent;
  private modalSize = "lg";

  public fromColumnsNames: Array<Select2OptionData>;
  public toColumnsNames: Array<Select2OptionData>;
  public columnsSelectOptions: Select2Options;
  public selectedFromColumns: string[];
  public selectedToColumns: string[];
  private edgeClassesNames;

  constructor() {

    this.activeFormNewEdge = "1";
    this.directions = ["direct", "inverse"];

    this.columnsSelectOptions = {
      multiple: true,
      width: "100%"
    };

    this.initTempInfo();
  }

  initTempInfo() {

    // temp info setting
    this.tmpEdgeName = undefined;
    this.tmpEdgeDefinition = {properties: {}, isLogical: false};
    this.tmpEdgeMapping = {fromTable: undefined, fromColumns: undefined, toTable: undefined, toColumns: undefined, direction: "direct"};
    this.fromVertex = undefined;
    this.toVertex = undefined;

    // reset of structures for the select2
    this.selectedFromColumns = [];
    this.selectedToColumns = [];

    // reset of edge classes' names
    this.edgeClassesNames = [];
  }

  setGraphComponent(graphComponent) {
    this.graphComponent = graphComponent;
  }

  openAddEdgeModal(fromVertex, toVertex, edgeClassesNames) {

    /**
     * Preparing data
     */

    this.fromVertex = fromVertex;
    this.toVertex = toVertex;

    // initializing from columns
    this.fromColumnsNames = [];   // to clean old data and update the child component
    var fromPropsNames = Object.keys(fromVertex.properties);
    for(var i=0; i<fromPropsNames.length; i++) {
      var currPropName = fromPropsNames[i];
      var currPropertyDef = fromVertex.properties[currPropName];
      if(currPropertyDef.mapping) {
        var currOption = {
          id: currPropertyDef.mapping.columnName,
          text: currPropertyDef.mapping.columnName
        }
        this.fromColumnsNames[i] = currOption;
      }
    }

    // initializing to columns
    this.toColumnsNames = [];   // to clean old data and update the child component
    var toPropsNames = Object.keys(toVertex.properties);
    for(var i=0; i<toPropsNames.length; i++) {
      var currPropName = toPropsNames[i];
      var currPropertyDef = toVertex.properties[currPropName];
      if(currPropertyDef.mapping) {
        var currOption = {
          id: currPropertyDef.mapping.columnName,
          text: currPropertyDef.mapping.columnName
        }
        this.toColumnsNames[i] = currOption;
      }
    }

    // setting edge classes' names
    this.edgeClassesNames = edgeClassesNames;

    // initializing from and to tables' names
    var tmpFromTable = this.fromVertex.mapping.sourceTables[0].tableName;   // ok till we do not have aggregated vertex, mapped with several table
    var tmpToTable = this.toVertex.mapping.sourceTables[0].tableName;   // ok till we do not have aggregated vertex, mapped with several table
    this.tmpEdgeMapping.fromTable = tmpFromTable;
    this.tmpEdgeMapping.toTable = tmpToTable;

    this.addEdgeModal.open();

  }

  dismissEdgeAdding() {

    // cleaning temp info
    this.initTempInfo();
    this.activeFormNewEdge = "1";

    // Do nothing!

    this.addEdgeModal.dismiss();
    this.graphComponent.endEdgeCreation(undefined);

  }

  addNewEdge() {

    // build and return the new edge to the graph component
    this.tmpEdgeMapping.fromColumns = this.selectedFromColumns;
    this.tmpEdgeMapping.toColumns = this.selectedToColumns;
    var newEdge = {source: undefined, target: undefined};
    this.tmpEdgeDefinition.mapping = [];
    this.tmpEdgeDefinition.mapping.push(this.tmpEdgeMapping);
    newEdge[this.tmpEdgeName] = this.tmpEdgeDefinition;
    newEdge.source = this.fromVertex;
    newEdge.target = this.toVertex;

    // cleaning temp info
    this.initTempInfo();
    this.activeFormNewEdge = "1";

    this.addEdgeModal.close();
    this.graphComponent.endEdgeCreation(newEdge);
  }

  /**
   * Updates the set of from-columns when the input value changes.
   * @param e
   */
  updatedFromColumns(e: any) {
    this.selectedFromColumns = e.value;
  }

  /**
   * Updates the set of to-columns when the input value changes.
   * @param e
   */
  updatedToColumns(e: any) {
    this.selectedToColumns = e.value;
  }

}

angular.module('addEdgeModal.component', []).directive(
  `add-edge-modal`,
  downgradeComponent({component: AddEdgeModal}));


export {AddEdgeModal};
