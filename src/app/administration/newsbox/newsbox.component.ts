import {Component, OnInit, EventEmitter, Input, ViewChild} from '@angular/core';
import {AgentService,DBService} from "../../core/services/";


import * as $ from "jquery"

import {downgradeComponent} from '@angular/upgrade/static';

declare var angular:any

@Component({
  selector: 'news-box',
  templateUrl: "./newsbox.component.html",
  styleUrls: []
})

class NewsBoxComponent {

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
      this.ceNewsUrl = "http://orientdb.com/studio-news.php?version=" + this.orientdbVersion + "&edition=c";
      this.eeNewsUrl = "http://orientdb.com/studio-news.php?version=" + this.orientdbVersion + "&edition=e";
      this.makeRequestAccordingVersionAndEdition();
    });

  }

  makeRequestAccordingVersionAndEdition() {

    // agent
    this.agentService.isActive().then(() => {
      this.httpGetAsync(this.eeNewsUrl, this.assignResponseToTheBox);
    }).catch(() => {
      this.httpGetAsync(this.ceNewsUrl, this.assignResponseToTheBox);
    });
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
    xmlHttp.onreadystatechange = function() {
      if (xmlHttp.readyState == 4 && xmlHttp.status == 200) {
        callback.call(self, xmlHttp.responseText);
      }
      else {
        self.newsHTMLPlainText = self.errorHTMLPLainText;
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
  `news-box`,
  downgradeComponent({component: NewsBoxComponent}));


export {NewsBoxComponent};
