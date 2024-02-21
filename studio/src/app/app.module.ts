import { NgModule, ApplicationRef } from "@angular/core";
import {
  removeNgStyles,
  createNewHosts,
  createInputTransfer
} from "@angularclass/hmr";

/*
 * Platform and Environment providers/directives/pipes
 */
import { ENV_PROVIDERS } from "./environment";
import { APP_RESOLVER_PROVIDERS } from "./app.resolver";
import { APP_DECLARATIONS } from "./app.declarations";
import { APP_IMPORTS } from "./app.imports";
import { APP_PIPES } from "./app.pipe";

import { platformBrowserDynamic } from "@angular/platform-browser-dynamic";
import { BaseRequestOptions, RequestOptions } from "@angular/http";
import { UpgradeModule } from "@angular/upgrade/static";

declare var angular: angular.IAngularStatic;

// Application wide providers
const APP_PROVIDERS = [...APP_RESOLVER_PROVIDERS];
class CustomRequestOptions extends BaseRequestOptions {
  constructor() {
    super();

    this.headers.append("X-Requested-With", "XMLHttpRequest");
  }
}

/**
 * `AppModule` is the main entry point into Angular2's bootstraping process
 */

@NgModule({
  declarations: [APP_DECLARATIONS, ...APP_PIPES],
  imports: [
    // import Angular's modules
    APP_IMPORTS
  ],
  providers: [
    // expose our Services and Providers into Angular's dependency injection
    ENV_PROVIDERS,
    APP_PROVIDERS,
    { provide: RequestOptions, useClass: CustomRequestOptions }
  ],
  entryComponents: [...APP_DECLARATIONS]
})
export class AppModule {
  constructor(public appRef: ApplicationRef) {}

  hmrOnInit(store) {}

  ngDoBootstrap() {}

  hmrOnDestroy(store) {}

  hmrAfterDestroy(store) {}
}

platformBrowserDynamic()
  .bootstrapModule(AppModule)
  .then(platformRef => {
    const upgrade = platformRef.injector.get(UpgradeModule);

    angular.element(document.body).ready(function() {
      upgrade.bootstrap(document.body, ["OrientDBStudioApp"]);
    });
  });
