import {Component, Input, Output, NgZone, AfterViewChecked, EventEmitter} from '@angular/core';

import * as $ from "jquery"

import {downgradeComponent} from '@angular/upgrade/static';
import {TeleporterService} from '../../../core/services';
import {NotificationService} from "../../../core/services/notification.service";
import {AgentService} from "../../../core/services/agent.service";

declare var angular:any

@Component({
  selector: 'graph-model-panel',
  templateUrl: "./graphmodelpanel.component.html",
  styleUrls: []
})

class GraphModelPanelComponent {

  @Input() modellingConfig = this.modellingConfig !== 'undefined' ? this.modellingConfig : 'no config from parent.';
  @Output() modellingConfigChange = new EventEmitter();

  updateModellingConfig() {
    this.modellingConfig.vertices[0].name = "Graph Model"
    this.modellingConfigChange.next(this.modellingConfig);
  }

}

angular.module('ermodelpanel.component', []).directive(
  `er-model-panel`,
  downgradeComponent({component: GraphModelPanelComponent}));


export {GraphModelPanelComponent};
