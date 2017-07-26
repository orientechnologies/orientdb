import {Component} from '@angular/core';

import * as $ from "jquery"

import {downgradeComponent} from '@angular/upgrade/static';
import {AgentService} from '../../core/services';

declare var angular:any

@Component({
  selector: 'importmanager',
  templateUrl: "./importmanager.component.html",
  styles: [
    '.vertical-van-tab {padding-left: 0px}',
    '.tabs-left {border-bottom: none; padding-top: 2px; border-right: 1px solid #ddd;}',
    '.tabs-left>li {float: none; margin-bottom: 2px;}',
    '.tabs-left>li {margin-right: -1px;}',
    '.tabs-left>li.active>a, .tabs-left>li.active>a:hover, .tabs-left>li.active>a:focus {border-bottom-color: #ddd; border-right-color: transparent;}',
    '.tabs-left>li>a {border-radius: 4px 0 0 4px; margin-right: 0; padding-left: 10px; display:block;}',

    '.not-allowed {opacity: 0.3; cursor: not-allowed;}',
    '.importer-description {margin: 20px; margin-bottom: 40px; margin-left: 15px;}',
    'img {float: left; border: none; margin: 15px;}',
    'li>a {font-size: 15px;}',
    'ul {list-style-type: none}'
  ]
})

class ImportManagerComponent {

  private currentTab = 'home';
  private isEE;
  private hints;

  constructor(private agentService: AgentService) {
    agentService.isActive().then(() => {
      this.isEE = true;
    }).catch(() => {
      this.isEE = false;
    })

    this.hints = {
      importDbFromSQL: "This is the Enterprise Edition, so you can <b>migrate</b> and <b>synch</b> your SQL database with <b>Teleporter</b>.",
      downloadEE: "You can <b>migrate</b> your SQL database with <b>Teleporter</b> but you <b>cannot synch</b> it as this is the Community Edition.<br/> " +
      "You can enjoy all the features simply downloading the <a href='http://orientdb.com/orientdb-enterprise/?#matrix'>Enterprise Edition.</a>"
    }
  }

  enablePopovers() {
    (<any>$('[data-toggle="popover"]')).popover({
      title: '',
      placement: 'right',
      trigger: 'focus'
    });
  }

  setTab(importer) {
    this.currentTab = importer;
  }

  isActive(tabName) {
    if(tabName === 'home' && this.currentTab === 'home') {
      return 'active';
    }
    else if(tabName === 'teleporter' && this.currentTab === 'teleporter') {
      return 'active';
    }
    else if(tabName === 'neo4jImporter' && this.currentTab === 'neo4jImporter') {
      return 'active';
    }
    return '';
  }

}

angular.module('importmanager.component', []).directive(
  `importmanager`,
  downgradeComponent({component: ImportManagerComponent}));


export {ImportManagerComponent};
