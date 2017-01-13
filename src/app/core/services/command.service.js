import {downgradeInjectable} from '@angular/upgrade/static';
import {Http} from '@angular/http';
import 'rxjs/add/operator/toPromise';

import {API} from '../../../constants';

class CommandService {
  constructor(http) {
    this.http = http;
  }


  command({db, language, query, limit}) {
    let startTime = new Date().getTime();
    limit = limit || 20;
    language = language || 'sql';
    let url = API + 'command/' + db + "/" + language + "/-/" + limit + '?format=rid,type,version,class,graph';
    query = query.trim();
    return this.http.post(url, query).toPromise();
  }
}

CommandService.parameters = [[Http]];

angular.module('command.services', []).factory(
  `CommandService`,
  downgradeInjectable(CommandService));

export {CommandService};
