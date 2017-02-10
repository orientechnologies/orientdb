import {downgradeInjectable} from '@angular/upgrade/static';
import {Injectable} from "@angular/core";

declare var angular: any;

@Injectable()
export class FormatArrayPipe {

  transform(input) {
    if (input instanceof Array) {
      var output = "";
      input.forEach(function (e, idx) {
        output += (idx > 0 ? ", " : " ") + e;
      })
      return output;
    } else {
      return input;
    }
  }
}

angular.module('legacy.filters', []).factory(
  `FormatArrayPipe`,
  downgradeInjectable(FormatArrayPipe));

