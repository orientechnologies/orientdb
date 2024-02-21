import * as data from "../../translations/hint.json";
import { Pipe } from "@angular/core";

@Pipe({
  name: "translate"
})
export class TranslationPipe {
  transform(input, links) {
    return data[input] || input;
  }
}
