import {downgradeInjectable} from '@angular/upgrade/static';
import {Http} from '@angular/http';
import 'rxjs/add/operator/toPromise';

import {Injectable} from "@angular/core";
import {ProfilerService} from '../../core/services/profiler.service';
declare var angular:any

@Injectable()
class AgentService {

  private agent;

  constructor(private http: Http, private profilerService: ProfilerService) {
    this.agent = {
      active: null
    }
  }

  isActive() {

    if (this.agent.active == null) {
      return this.profilerService.metadata().then((data) => {
        this.agent.active = true;
      }).catch(function (err) {
        this.agent.active = false;
      })
    }
    return this.agent.active;
  }

}

angular.module('command.services', []).factory(
  `AgentService`,
  downgradeInjectable(AgentService));

export {AgentService};
