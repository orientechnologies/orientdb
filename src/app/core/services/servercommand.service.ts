import {downgradeInjectable} from '@angular/upgrade/static';
import {Http} from '@angular/http';
import 'rxjs/add/operator/toPromise';

import {API} from '../../../constants';
import {Injectable} from "@angular/core";
declare var angular : any;

@Injectable()
class ServerCommandService {
  constructor(private  http : Http) {
  }


  serverCommand(params : {query : any, limit?}) {
    let startTime = new Date().getTime();
    params.limit = params.limit || 20;
    let url = API + 'servercommand';
    params.query = params.query.trim();
    return this.http.post(url, params.query).toPromise();
  }

}


angular.module('servercommand.services', []).factory(
  `ServerCommandService`,
  downgradeInjectable(ServerCommandService));

export {ServerCommandService};
