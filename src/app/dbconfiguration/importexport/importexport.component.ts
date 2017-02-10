import {Component, Input} from '@angular/core';
import {downgradeComponent} from '@angular/upgrade/static';
import {DBService} from '../../core/services';

declare var angular : any;
@Component({
  selector: 'import-export',
  templateUrl: "./import.export.component.html",
  styleUrls: [
  ]

})
export class ImportExport {

  @Input()
  protected db: string;

  constructor(private dbService : DBService) {
  }

  exportDatabase() {

    this.dbService.exportDB(this.db);

  }
}


angular.module('dbconfig.components', []).directive(
  `importExport`,
  downgradeComponent({component: ImportExport, inputs: ["db"]}));


