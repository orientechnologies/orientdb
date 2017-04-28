import {Component, Input, Output, NgZone, AfterViewChecked, EventEmitter} from '@angular/core';

import * as $ from "jquery"

import {downgradeComponent} from '@angular/upgrade/static';
import {TeleporterService} from '../../../core/services';
import {NotificationService} from "../../../core/services/notification.service";
import {AgentService} from "../../../core/services/agent.service";

declare var angular:any

@Component({
  selector: 'detail-panel',
  templateUrl: "detailpanel.component.html",
  styleUrls: []
})

class DetailPanelComponent {

  @Input() modellingConfig = this.modellingConfig !== 'undefined' ? this.modellingConfig : 'no config from parent.';
  @Output() modellingConfigChange = new EventEmitter();

  updateModellingConfig() {
    this.modellingConfig.vertices[0].name = "Detail";
    this.modellingConfigChange.next(this.modellingConfig);
  }

}

angular.module('detailpanel.component', []).directive(
  `detail-panel`,
  downgradeComponent({component: DetailPanelComponent}));


export {DetailPanelComponent};
