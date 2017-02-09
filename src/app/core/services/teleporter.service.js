import {downgradeInjectable} from '@angular/upgrade/static';
import {Http} from '@angular/http';
import 'rxjs/add/operator/toPromise';

import {API} from '../../../constants';


class TeleporterService {

  constructor(http) {
    this.http = http;
  }

  drivers() {
    let url = API + 'teleporter/drivers';
    return this.http.get(url).toPromise().then((data) => {
      return data.json();
    });
  }

  launch(params) {
    let url = API + 'teleporter/job';
    return this.http.post(url, params).toPromise().then((data) => {
      return data.json();
    });
  }

  test(params) {
    let url = API + 'teleporter/test';
    return this.http.post(url, params).toPromise().then((data) => {
      return data.json();
    });
  }

  status() {
    let url = API + 'teleporter/status';
    return this.http.get(url).toPromise().then((data) => {
      return data.json();
    });
  }

}

TeleporterService.parameters = [[Http]];

angular.module('command.services', []).factory(
  `TeleporterService`,
  downgradeInjectable(TeleporterService));

export {TeleporterService};
