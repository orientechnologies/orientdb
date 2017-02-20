import {downgradeInjectable} from '@angular/upgrade/static';
import {Http} from '@angular/http';
import 'rxjs/add/operator/toPromise';

import {API} from '../../../constants';
import {Injectable} from "@angular/core";
declare var angular:any

@Injectable()
class ProfilerService {

  constructor(private http: Http) {
  }

  profilerData(params) {
    let url = API + 'sqlProfiler/' + params.db;
    return this.http.get(url).toPromise().then((data) => {
      return data.json();
    });
  }

  reset(params) {
    let url = API + 'sqlProfiler/' + params.db + '/reset';
    return this.http.get(url).toPromise().then((data) => {
      return data.json();
    });
  }

  metadata() {
    let url = API + 'profiler/metadata';
    return this.http.get(url).toPromise().then((data) => {
      return data.json();
    });
  }

  realtime() {
    let url = API + 'profiler/realtime';
    return this.http.get(url).toPromise().then((data) => {
      return data.json();
    });
  }
}

angular.module('command.services', []).factory(
  `ProfilerService`,
  downgradeInjectable(ProfilerService));

export {ProfilerService};
