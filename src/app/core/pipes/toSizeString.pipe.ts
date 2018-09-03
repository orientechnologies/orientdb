import { downgradeInjectable } from "@angular/upgrade/static";
import { Injectable, Pipe } from "@angular/core";
import * as filesize from "filesize";

declare var angular: any;

@Pipe({
  name: "toSizeString"
})
export class ToSizeString {
  transform(input) {
    if (input != null) {
      return filesize(input);
    }
    return input;
  }
}
