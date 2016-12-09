import {Component} from '@angular/core';
import {downgradeComponent} from '@angular/upgrade/static';
import {DBService} from '../../core/services';

import template from './import.export.component.html';


class ImportExport {

  constructor(dbService) {
    this.dbService = dbService;
  }

  exportDatabase() {

    this.dbService.exportDB(this.db);

  }
}

ImportExport.parameters = [[DBService]];

ImportExport.annotations = [new Component({
  selector: "import-export",
  template: template,
  inputs: ["db"]
})]


angular.module('dbconfig.components', []).directive(
  `importExport`,
  downgradeComponent({component: ImportExport, inputs: ["db"]}));


export {ImportExport};
