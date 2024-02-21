import {Component, OnInit, Input, OnChanges, SimpleChanges} from '@angular/core';
import {AgentService, DBService} from "../../core/services/";


import * as $ from "jquery"

import {downgradeComponent} from '@angular/upgrade/static';

declare var angular: any

@Component({
  selector: 'news-box',
  templateUrl: "./newsbox.component.html",
})

class NewsBoxComponent implements OnInit {


  @Input() enterprise: boolean;
  @Input() boxHeight: String;
  @Input() boxMarginTop: String;

  private orientdbVersion;

  private ceNewsUrl;
  private eeNewsUrl;

  private newsHTMLPlainText;
  private errorHTMLPLainText;

  constructor(private agentService: AgentService, private dbService: DBService) {

    this.errorHTMLPLainText = "<h2> Error: news cannot be loaded! </h2><br/>" +
      "Try checking your internet connection.";

    // inferring orientDB version
    this.dbService.getServerVersion().then((data) => {
      this.orientdbVersion = data;
      this.ceNewsUrl = "https://orientdb.com/studio-news.php?version=" + this.orientdbVersion + "&edition=c";
      this.eeNewsUrl = "https://orientdb.com/studio-news.php?version=" + this.orientdbVersion + "&edition=e";
      this.makeRequestAccordingVersionAndEdition();
    });

  }

  ngOnInit() {
    if (this.boxHeight === undefined) {
      this.boxHeight = "420px";
    }

    if (this.boxMarginTop === undefined) {
      this.boxMarginTop = "40px";
    }
  }

  makeRequestAccordingVersionAndEdition() {
    
    if (this.enterprise) {
      this.httpGetAsync(this.eeNewsUrl, this.assignResponseToTheBox);
    } else {
      this.httpGetAsync(this.ceNewsUrl, this.assignResponseToTheBox);
    }
  }

  enablePopovers() {
    (<any>$('[data-toggle="popover"]')).popover({
      title: '',
      placement: 'right',
      trigger: 'focus'
    });
  }

  httpGetAsync(theUrl, callback) {
    var self = this;
    var xmlHttp = new XMLHttpRequest();
    xmlHttp.onreadystatechange = function () {
      if (xmlHttp.readyState == 4) {
        if (xmlHttp.status == 200) {
          callback.call(self, xmlHttp.responseText);
        }
        else {
          self.newsHTMLPlainText = self.errorHTMLPLainText;
        }
      }
    }
    xmlHttp.open("GET", theUrl, true); // true for asynchronous
    xmlHttp.send(null);
  }

  /**
   * Callback function
   * @param htmlText
   */
  assignResponseToTheBox(htmlText) {
    this.newsHTMLPlainText = htmlText;
  }


}

angular.module('newsbox.component', []).directive(
  `newsBox`,
  downgradeComponent({component: NewsBoxComponent, inputs: ["enterprise", "boxHeight", "boxMarginTop"]}));


export {NewsBoxComponent};
