import {Component} from '@angular/core';

import * as $ from "jquery"

import {downgradeComponent} from '@angular/upgrade/static';
import {NotificationService} from "../../core/services/notification.service";
import {AgentService} from "../../core/services/agent.service";

declare var angular:any

@Component({
  selector: 'scheduler',
  templateUrl: "./scheduler.component.html",
  styleUrls: []
})

class SchedulerComponent {

  constructor(private notification: NotificationService, private agentService: AgentService) {

    // agent
    // this.agentService.isActive().then(() => {
    //   this.init();
    // });

  }

  init() {

  }


}

angular.module('scheduler.components', []).directive(
  `scheduler`,
  downgradeComponent({component: SchedulerComponent}));


export {SchedulerComponent};
