import {Component, Input} from '@angular/core';
import {downgradeComponent} from '@angular/upgrade/static';
import {DBService} from '../../core/services';

import * as angular from 'angular';

@Component({
  selector: 'import-export',
  templateUrl: "./import.export.component.html",
  styleUrls: []

})
class ImportExportComponent {

  @Input()
  protected db: string;

  constructor(private dbService: DBService) {
  }

  exportDatabase() {

    this.dbService.exportDB(this.db);

  }
}


angular.module('dbconfig.components', []).directive(
  `importExport`,
  downgradeComponent({component: ImportExportComponent, inputs: ["db"]}));

export  {ImportExportComponent};
