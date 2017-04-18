import {Component, Input, Output, NgZone, AfterViewChecked, EventEmitter} from '@angular/core';

import * as $ from "jquery"

import {downgradeComponent} from '@angular/upgrade/static';
import {TeleporterService} from '../../../core/services';
import {NotificationService} from "../../../core/services/notification.service";
import {AgentService} from "../../../core/services/agent.service";

declare var angular:any

@Component({
  selector: 'er-model-panel',
  templateUrl: "./ermodelpanel.component.html",
  styleUrls: []
})

class ERModelPanelComponent {

  @Input() modellingConfig = this.modellingConfig !== 'undefined' ? this.modellingConfig : 'no config from parent.';
  @Output() modellingConfigChange = new EventEmitter();

  updateModellingConfig() {
    this.modellingConfig.vertices[0].name = "ER Model"
    this.modellingConfigChange.next(this.modellingConfig);
  }

}

angular.module('ermodelpanel.component', []).directive(
  `er-model-panel`,
  downgradeComponent({component: ERModelPanelComponent}));


export {ERModelPanelComponent};
