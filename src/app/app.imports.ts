import {BrowserModule} from '@angular/platform-browser';
import {FormsModule} from '@angular/forms';
import {HttpModule} from '@angular/http';
import {CommonModule} from '@angular/common';
import {UpgradeModule} from "@angular/upgrade/src/aot/upgrade_module";
import {TagInputModule} from 'ng2-tag-input';
import {Select2Module} from 'ng2-select2';
import {Ng2Bs3ModalModule} from 'ng2-bs3-modal/ng2-bs3-modal';



export const APP_IMPORTS = [
  BrowserModule,
  CommonModule,
  FormsModule,
  HttpModule,
  UpgradeModule,
  Select2Module,
  TagInputModule,
  Ng2Bs3ModalModule
];
