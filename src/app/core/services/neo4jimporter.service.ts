import {downgradeInjectable} from '@angular/upgrade/static';
import {Http} from '@angular/http';
import 'rxjs/add/operator/toPromise';

import {API} from '../../../constants';
import {Injectable} from "@angular/core";
import { Headers } from "@angular/http";
declare var angular:any

@Injectable()
class Neo4jImporterService {

  constructor(private http: Http) {
  }

  launch(params) {
    let url = API + 'neo4j-importer/job';
    return this.http.post(url, params,this.getOptions()).toPromise().then((data) => {
      return data.json();
    });
  }

  testConnection(params) {
    let url = API + 'neo4j-importer/test';
    return this.http.post(url, params,this.getOptions()).toPromise().then((data) => {
      return data.json();
    });
  }

  status() {
    let url = API + 'neo4j-importer/status';
    return this.http.get(url,this.getOptions()).toPromise().then((data) => {
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

angular.module('command.services', []).factory(
  `Neo4jImporterService`,
  downgradeInjectable(Neo4jImporterService));

export {Neo4jImporterService};


