import {downgradeInjectable} from '@angular/upgrade/static';
import {Http} from '@angular/http';
import 'rxjs/add/operator/toPromise';

import {API} from '../../../constants';
import {Injectable} from "@angular/core";
declare var angular:any

@Injectable()
class EtlService {

  constructor(private http: Http) {
  }

  launch(params) {
    let url = API + 'etl/job';
    return this.http.post(url, params).toPromise().then((data) => {
      return data.json();
    });
  }

  status() {
    let url = API + 'etl/status';
    return this.http.get(url).toPromise().then((data) => {
      return data.json();
    });
  }

}


angular.module('command.services', []).factory(
  `EtlService`,
  downgradeInjectable(EtlService));

export {EtlService};
