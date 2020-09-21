import {Component, Input, ViewChild, OnInit, ElementRef} from '@angular/core';

import * as $ from "jquery"

import {downgradeComponent} from '@angular/upgrade/static';
import {NotificationService} from "../../core/services/notification.service";
import {AgentService} from "../../core/services/agent.service";
import {ServerCommandService} from "../../core/services/servercommand.service";


import * as Codemirror from "codemirror";


declare var angular:any

@Component({
  selector: 'servercommands',
  templateUrl: "./servercommands.component.html",
  styleUrls: []
})

class ServerCommandsComponent implements OnInit{

  codemirror: any;

  private options = {
                        lineWrapping: true,
                        lineNumbers: true,
                        readOnly: false
                      };

  @ViewChild("codemirror", { read: ElementRef })
  textArea: ElementRef;


  @Input()
  private queryText: string = "";


  constructor(private notification: NotificationService, private agentService: AgentService, private serverCommandService: ServerCommandService) {

  }

  ngOnInit() {
    this.codemirror = Codemirror.fromTextArea(
      this.textArea.nativeElement,
      this.options
    );
    this.codemirror.setValue(this.queryText);
    this.codemirror.on('change', () => {
      this.queryText = this.codemirror.getValue();
      console.log("changed to "+this.codemirror.getValue());
    });
  }

  test() {
    console.log("OK, it works");
  }

  query() {
    console.log("sending command: " + this.queryText);
    var result = this.serverCommandService.serverCommand({"command": this.queryText.trim()});
    result.then((val) => console.log(val));
  }

}

angular.module('servercommands.components', []).directive(
  `servercommands`,
  downgradeComponent({component: ServerCommandsComponent}));


export {ServerCommandsComponent};
