import { downgradeInjectable } from "@angular/upgrade/static";

import { API, STUDIO_VERSION } from "../../../constants";
import { Injectable } from "@angular/core";

import { Headers } from "@angular/http";
declare var angular: any;

@Injectable()
class WikiService {
  version: string;
  wikiBase: string;

  constructor() {
    this.version =
      STUDIO_VERSION.indexOf("SNAPSHOT") == -1 ? STUDIO_VERSION : "last";
    this.wikiBase = "http://www.orientdb.com/docs/" + this.version + "/";
  }

  resolveWiki(urlWiki) {
    return this.wikiBase + urlWiki;
  }
}

angular
  .module("command.services", [])
  .factory(`WikiService`, downgradeInjectable(WikiService));

export { WikiService };
