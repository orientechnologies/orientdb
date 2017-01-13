import {downgradeInjectable} from '@angular/upgrade/static';

class FormatArrayPipe {

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

export {FormatArrayPipe};
