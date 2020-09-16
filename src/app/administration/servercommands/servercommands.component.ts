import {Component} from '@angular/core';

import * as $ from "jquery"

import {downgradeComponent} from '@angular/upgrade/static';
import {NotificationService} from "../../core/services/notification.service";
import {AgentService} from "../../core/services/agent.service";
import {ServerCommandService} from "../../core/services/servercommand.service";

declare var angular:any

@Component({
  selector: 'servercommands',
  templateUrl: "./servercommands.component.html",
  styleUrls: []
})

class ServerCommandsComponent {

  constructor(private notification: NotificationService, private agentService: AgentService, private serverCommandService: ServerCommandService) {

    // agent
    // this.agentService.isActive().then(() => {
    //   this.init();
    // });

  }

  init() {

  }

  test() {
    console.log("OK, it works");
  }

}

angular.module('servercommands.components', []).directive(
  `servercommands`,
  downgradeComponent({component: ServerCommandsComponent}));


export {ServerCommandsComponent};
