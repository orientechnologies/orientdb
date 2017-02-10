import {downgradeInjectable} from '@angular/upgrade/static';
import {Http} from '@angular/http';
import 'rxjs/add/operator/toPromise';

import {API} from '../../../constants';
import {Injectable} from "@angular/core";
declare var angular : any;

@Injectable()
class CommandService {
  constructor(private  http : Http) {
  }


  command(params : {db, language? : any, query : any, limit?}) {
    let startTime = new Date().getTime();
    params.limit = params.limit || 20;
    params.language = params.language || 'sql';
    let url = API + 'command/' + params.db + "/" + params.language + "/-/" + params.limit + '?format=rid,type,version,class,graph';
    params.query = params.query.trim();
    return this.http.post(url, params.query).toPromise();
  }
}


angular.module('command.services', []).factory(
  `CommandService`,
  downgradeInjectable(CommandService));

export {CommandService};
