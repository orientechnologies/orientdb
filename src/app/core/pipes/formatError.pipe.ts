import {downgradeInjectable} from '@angular/upgrade/static';
import {Injectable} from "@angular/core";


declare var angular : any;
@Injectable()
class FormatErrorPipe {

  transform(input) {
    if (typeof input == 'string') {
      return input;
    } else if (typeof  input == 'object') {
      return input.errors[0].content;
    } else {
      return input;
    }
  }
}

angular.module('legacy.filters', []).factory(
  `FormatErrorPipe`,
  downgradeInjectable(FormatErrorPipe));

export {FormatErrorPipe};
