import {downgradeInjectable} from '@angular/upgrade/static';
import {Pipe} from '@angular/core'

@Pipe({
  name: "objectKeys"
})
class ObjectKeysPipe {

  transform(input)  {

    let keys = [];
    for (let key in input) {
      keys.push(key);
    }

    console.log(keys);
    return keys;
  }
}

export {ObjectKeysPipe};
