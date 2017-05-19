import {Component, Input, Output, EventEmitter, OnChanges, AfterViewChecked} from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';


import * as $ from "jquery"

import {downgradeComponent} from '@angular/upgrade/static';
import {NotificationService} from "../../../core/services/notification.service";

declare var angular:any
import * as d3 from 'd3';


@Component({
  selector: 'detail-panel',
  templateUrl: "./detailpanel.component.html",
  styles: [
    '.propAndColumnTable {max-width: 480px; table-layout: fixed}',
    '.propAndColumnTable td, .propAndColumnTable th {overflow-x:auto; height: 40px;}',
    '.firstColumn {width: 50%;}',
    '.secondColumn {width: 25%;}',
    '.thirdColumn {width: 25%;}',

    '.hideOverflow {max-width: 100%; overflow:hidden; white-space:nowrap; text-overflow:ellipsis; display: inline-block}',

    '.OneNRelTable {width: 150px; empty-cells: show}',
    '.NNRelTable {width: 100px; empty-cells: show}',
    '.OneNRelInfoTable {width: 150px; empty-cells: show;table-layout:fixed;}',
    '.NNRelInfoTable {width: 100px; empty-cells: show; table-layout:fixed;}',
    '.OneNRelTableLabel {width: 150px; overflow: hidden; white-space: nowrap; text-overflow: ellipsis; display: inline-block;}',
    '.NNRelTableLabel {width: 100px; overflow: hidden; white-space: nowrap; text-overflow: ellipsis; display: inline-block;}',
    '.OneNRelInfoTable ul, .NNRelInfoTable ul {padding-left: 2px;}',
    'li {padding-left: 2px;list-style-type: disc; list-style-position: inside; overflow: hidden; white-space: nowrap; text-overflow: ellipsis; }'
]
})

class DetailPanelComponent implements OnChanges, AfterViewChecked {

  @Input() modellingConfig = this.modellingConfig !== 'undefined' ? this.modellingConfig : 'no config from parent.';
  @Output() modellingConfigChange = new EventEmitter();

  @Input() selectedElement;

  private propertiesName;
  private edgeName;

  // 1-N Relationships Info
  private fromTableInfo;
  private toTableInfo;

  // N-N Relationship Info
  private leftTableInfo;
  private joinTableInfo;
  private rightTableInfo;


  constructor() {
    this.edgeName = undefined;
    this.propertiesName = [];
  }

  ngAfterViewChecked() {
    this.enablePopovers();
  }


  enablePopovers() {
    (<any>$('[data-toggle="popover"]')).popover();
    // (<any>$('[data-toggle="popover"]')).popover().css("max-width", "100%");

    // (<any>$('[data-toggle="popover"]')).on('shown.bs.popover', function () {
    //   $($(this)).css('max-width', 100);
    // });

    // $('[data-toggle="popover"]').on('shown.bs.popover', function () {
    //   console.log("Element:");
    //   console.log(this);
    //   console.log("Element next:");
    //   console.log($($(this).next()));
    //   console.log("Popover width:");
    //   console.log($($(this).next()).width());
    // });

    // $('[data-toggle="popover"]').on('shown.bs.popover', function () {
    //   var popoverID = $(this).attr('aria-describedby');
    //   $("#"+popoverID).css("max-width", "100%");
    //   console.log($(this));
    //   console.log($("#"+popoverID).width());
    // });
  }

  ngOnChanges(changes) {

    if(changes.selectedElement && this.selectedElement) {


      if(!this.selectedElement.name) {

        // when an edge is selected we store the edge name
        this.edgeName = Object.keys(this.selectedElement)[0];

        // filling properties array
        this.propertiesName = [];
        var props = this.selectedElement[this.edgeName].properties;
        for(var i=0; i<Object.keys(props).length; i++) {
          this.propertiesName.push(Object.keys(props)[i]);
        }
      }
      else {

        // when a vertex is selected we delete the edge name
        this.edgeName = undefined;

        // filling properties array
        this.propertiesName = [];
        var props = this.selectedElement.properties;
        for(var i=0; i<Object.keys(props).length; i++) {
          this.propertiesName.push(Object.keys(props)[i]);
        }
      }
    }
  }

  renderNNRelationshipInfo(relationship, direction) {

    if(direction === "left") {

      // deleting rightTableInfo
      this.rightTableInfo = undefined;

      this.leftTableInfo = {label: "ToColumns", columns: []};
      for (var i = 0; i < relationship.fromColumns.length; i++) {
        this.leftTableInfo.columns.push(relationship.fromColumns[i]);
      }
    }
    else if(direction === "right") {

      // deleting leftTableInfo
      this.leftTableInfo = undefined;

      this.rightTableInfo = {label: "ToColumns", columns: []};
      for (var i = 0; i < relationship.toColumns.length; i++) {
        this.rightTableInfo.columns.push(relationship.toColumns[i]);
      }
    }

    this.joinTableInfo = {label: "FromColumns", columns: []};

    if(direction === "left") {
      for (var i = 0; i < relationship.joinTable.fromColumns.length; i++) {
        this.joinTableInfo.columns.push(relationship.joinTable.fromColumns[i]);
      }
    }
    else if(direction === "right") {
      for (var i = 0; i < relationship.joinTable.toColumns.length; i++) {
        this.joinTableInfo.columns.push(relationship.joinTable.toColumns[i]);
      }
    }

  }

  changeSelectedElement(e) {

    // deleting info about Relationships
    this.leftTableInfo = undefined;
    this.joinTableInfo = undefined;
    this.rightTableInfo = undefined;
    this.fromTableInfo = undefined;
    this.toTableInfo = undefined;
  }

  render1NRelationshipInfo(relationship) {

    this.fromTableInfo = {label: "FromColumns", columns: []};
    for (var i = 0; i < relationship.fromColumns.length; i++) {
      this.fromTableInfo.columns.push(relationship.fromColumns[i]);
    }

    this.toTableInfo = {label: "ToColumns", columns: []};
    for (var i = 0; i < relationship.toColumns.length; i++) {
      this.toTableInfo.columns.push(relationship.toColumns[i]);
    }

  }

}

angular.module('detailpanel.component', []).directive(
  `detail-panel`,
  downgradeComponent({component: DetailPanelComponent}));


export {DetailPanelComponent};
