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

  codemirror_query: any;
  codemirror_result: any;

  result: any = {};

  private options = {
                        lineWrapping: true,
                        lineNumbers: true,
                        readOnly: false
                      };

  private resultOptions = {
                        lineWrapping: true,
                        lineNumbers: false,
                        mode: {name: "javascript", json: true},
                        readOnly: true
                      };

  @ViewChild("codemirror_query", { read: ElementRef })
  textArea: ElementRef;

  @ViewChild("codemirror_result", { read: ElementRef })
  resultArea: ElementRef;

  @Input()
  private queryText: string = "";


  constructor(private notification: NotificationService, private agentService: AgentService, private serverCommandService: ServerCommandService) {

  }

  ngOnInit() {
    this.codemirror_query = Codemirror.fromTextArea(
      this.textArea.nativeElement,
      this.options
    );
    this.codemirror_query.setValue(this.queryText);
    this.codemirror_query.on('change', () => {
      this.queryText = this.codemirror_query.getValue();
//      console.log("changed to "+this.codemirror_query.getValue());
    });

    this.codemirror_result = Codemirror.fromTextArea(
      this.resultArea.nativeElement,
      this.resultOptions
    );
  }

  query() {
    this.codemirror_result.setValue("Executing...");
    //console.log("sending command: " + this.queryText);
    var result = this.serverCommandService.serverCommand({"command": this.queryText.trim()});
    result.then((val) => {
        var jsonResult = JSON.stringify(val, null, 2)
        //console.log(jsonResult);
        this.result = jsonResult;
        this.codemirror_result.setValue(jsonResult);
      }).catch((error) => {
        var errorString = JSON.stringify(error);
        this.result = errorString;
        this.codemirror_result.setValue(errorString);
      });
  }

}

angular.module('servercommands.components', []).directive(
  `servercommands`,
  downgradeComponent({component: ServerCommandsComponent}));


export {ServerCommandsComponent};
