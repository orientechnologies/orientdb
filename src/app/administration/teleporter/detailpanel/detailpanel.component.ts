import {Component, Input, Output, EventEmitter, OnChanges} from '@angular/core';

import * as $ from "jquery"

import {downgradeComponent} from '@angular/upgrade/static';
import {NotificationService} from "../../../core/services/notification.service";

declare var angular:any

@Component({
  selector: 'detail-panel',
  templateUrl: "detailpanel.component.html",
  styleUrls: []
})

class DetailPanelComponent implements OnChanges {

  @Input() modellingConfig = this.modellingConfig !== 'undefined' ? this.modellingConfig : 'no config from parent.';
  @Output() modellingConfigChange = new EventEmitter();

  @Input() selectedElement;

  private edgeName;

  constructor() {
    this.edgeName = undefined;
  }


  ngOnChanges(changes) {

    if(changes.selectedElement && this.selectedElement) {

      // when and edge is selected we store the edge name
      if(!this.selectedElement.name) {
        this.edgeName = Object.keys(this.selectedElement)[0];
      }
      else {
        this.edgeName = undefined;
      }
    }
  }

}

angular.module('detailpanel.component', []).directive(
  `detail-panel`,
  downgradeComponent({component: DetailPanelComponent}));


export {DetailPanelComponent};
