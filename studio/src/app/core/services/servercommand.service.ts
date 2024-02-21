import {downgradeInjectable} from '@angular/upgrade/static';
import {Http, Headers} from '@angular/http';

import 'rxjs/add/operator/toPromise';

import {API} from '../../../constants';
import {Injectable} from "@angular/core";
declare var angular : any;

@Injectable()
class ServerCommandService {
  constructor(private  http : Http) {
  }


  serverCommand(params : {command : any, limit?}) {
    let url = API + 'servercommand';

    return this.http.post(url, {command: params.command}, this.getOptions())
          .toPromise()
          .then(data => {
            return data.json();
          });
  }

  getOptions() {
    let headers = new Headers({
      'Authorization': localStorage.getItem("SimpleAuth")
    });
    return {
      headers: headers
    }
  }
}


angular.module('servercommand.services', []).factory(
  `ServerCommandService`,
  downgradeInjectable(ServerCommandService));

export {ServerCommandService};
