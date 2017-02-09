import {downgradeInjectable} from '@angular/upgrade/static';
import {Pipe} from '@angular/core'

class ObjectKeysPipe {

  transform(input)  {
    let keys = [];
    for (let key in input) {
      keys.push(key);
    }
    return keys;
  }
}

ObjectKeysPipe.annotations = [new Pipe({
  name: "objectKeys"
})]

export {ObjectKeysPipe};
