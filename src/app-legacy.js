import 'jquery';
import angular from 'angular';



import 'core-js';
import 'babel-polyfill';
import 'zone.js';
import 'ie-shim';
import 'reflect-metadata';
import 'angular-strap';
import 'angular-local-storage';
import 'angular-scroll';
import 'angular-animate';
import 'angular-sanitize';
import 'ui-select';
import 'angular-spectrum-colorpicker';
import 'angular-translate';
import 'angular-translate-loader-partial';
import 'ng-tags-input';
import 'angular-bootstrap-switch';
import 'bootstrap-switch';
import 'angular-smart-table';
import 'ng-table';

import 'd3';
import 'c3';
import 'spectrum-colorpicker';

import 'angular-spectrum-colorpicker';
import 'fullcalendar';


import  'angular-strap/dist/angular-strap.tpl';
import  'bootstrap';


// Vendor Style
import  'bootstrap/dist/css/bootstrap.css';
import  'bootstrap/dist/css/bootstrap-theme.min.css';
import  'bootstrap-switch/dist/css/bootstrap3/bootstrap-switch.min.css';
import  'font-awesome/css/font-awesome.css';

import  'spectrum-colorpicker/spectrum.css';
import  'ui-select/dist/select.min.css';

import 'ng-tags-input/build/ng-tags-input.min.css';
import 'ng-tags-input/build/ng-tags-input.bootstrap.min.css';
import 'c3/c3.min.css';
import 'fullcalendar/dist/fullcalendar.min.css';
import 'angular-motion/dist/angular-motion.min.css';




// Bundled Vendor

import './vendor/jquery.fonticonpicker/jquery.fonticonpicker.css';
import './vendor/jquery.fonticonpicker/jquery.fonticonpicker.inverted.css';
import './vendor/jquery.fonticonpicker/jquery.fonticonpicker.min';
import './vendor/jquery-cron-min';

// CONFIG
import routing from './app.config'
import NProgress from 'nprogress';

// CONTROLLERS

import HeaderController from './controllers/header-controller'
import LoginModule from './controllers/login-controller'

import DatabaseController from './controllers/database-controller'
import {CtrlName as DocumentController} from './controllers/document-controller'
import ServerController from './controllers/server-controller'
import VertexController from './controllers/graph-controller'
import FunctionController from './controllers/function-controller'
import UsersCtrl from './controllers/users-controller'
import AsideController from './controllers/aside-controller'
import NotificationController from './controllers/notification-controller'
import ConfigurationController from './controllers/configuration-controller'
import EEController from './controllers/ee-controller'
import SchemaController from './controllers/schema-controller'


// COMPONENTS Angular 1.5

import APP_COMPONENTS_LEGACY from './components';

// WIDGET

import CodeMirrorUI from './directives/ui-codemirror';
import BootstrapTabSet from './directives/tabset';
import Rendering from './directives/widget';
import StudioGraph from './directives/graph';


// FILTERS

import StudioFilters from './filters/filter'


//  STYLES

import './styles/main.css';
import './styles/layout.css';
import './styles/responsive.css';
import './styles/animation.css';
import './styles/graph.css';
import './styles/fontello-codes.css';
import './styles/fontello-embedded.css';
import './styles/fontello.css';
import './styles/animation.css';


let deps = [HeaderController,
  LoginModule,
  DatabaseController,
  DocumentController,
  ServerController,
  VertexController,
  FunctionController,
  UsersCtrl,
  AsideController,
  NotificationController,
  ConfigurationController,
  EEController,
  SchemaController,
  ...APP_COMPONENTS_LEGACY,
  'mgcrea.ngStrap',
  CodeMirrorUI,
  'LocalStorageModule',
  'aside.services',
  'graph.services',
  'icon.services',
  'history.services',
  'browse.services',
  'ee.services',
  'schema.services',
  'legacy.filters',
  BootstrapTabSet,
  'ngTable',
  'filters',
  'rendering',
  'graph',
  'duScroll',
  'dbconfig.components',
  'ui.select',
  'ngRoute',
  'ngAnimate',
  'ngSanitize',
  'angularSpectrumColorpicker',
  'pascalprecht.translate',
  'ngTagsInput',
  'frapontillo.bootstrap-switch',
  'smart-table'];


var App = angular.module('OrientDBStudioApp', deps);

App.config(routing);

App.run(["$rootScope", "$interval", "DatabaseApi", "Notification", "Spinner", "$templateCache", "Aside", function ($rootScope, $interval, DatabaseApi, Notification, Spinner, $templateCache, Aside) {
  $rootScope.$on('$routeChangeSuccess', function (event, currentRoute, oldRoute) {
    switch (currentRoute.templateUrl) {
      case 'views/login.html':
        $rootScope.bodyClass = 'landing-page';
        break;
      default:
        $rootScope.bodyClass = 'normal-page';
        break;
    }
    if (currentRoute.$$route.controller === 'ServerDashboardController') {
      $rootScope.$emit("servermgmt:open");
    } else {
      $rootScope.$emit("servermgmt:close");
    }
    if (oldRoute && currentRoute.originalPath != oldRoute.originalPath) {
      Notification.clear();
    }
    NProgress.done();
  });
  $rootScope.$on("$routeChangeStart", function (event, next, current) {
    Aside.destroy();
    NProgress.start();
    NProgress.set(0.2);
    NProgress.set(0.4);

  })

  var counter = 0;
  var limit = 10;
  var serverDown = false;


  $rootScope.$on('server:check', function () {
    checkServer();
  })


  function checkServer() {
    DatabaseApi.listDatabases().then(function (data) {
      $rootScope.$broadcast("server:up");
      counter = 0;
      serverDown = false;
    }, function error(data) {
      $rootScope.$broadcast("server:down");
      $rootScope.$broadcast("server:retry", limit - counter);
      counter = 0;
      serverDown = true;
    })
  }


  $interval(function () {

    if (counter === limit) {
      checkServer();
    } else {
      if (serverDown) {
        $rootScope.$broadcast("server:retry", limit - counter);
      }
      counter++;
    }

  }, 1000);

  $templateCache.put('popover/popover.tpl.html', '<div class="popover"><div class="arrow"></div><h3 class="popover-title" ng-bind="title" ng-show="title"></h3><div class="popover-content" ng-bind-html="content"></div></div>');
}])

$('body').on('keyup', function (e) {

  if (e.keyCode == 27) {
    $('.modal-backdrop').click()
  }
})


// let AppModule = NgModule({
//   imports: [BrowserModule, UpgradeModule, HttpModule],
//   providers: [...APP_RESOLVER_PROVIDERS],
//   declarations: [...APP_DECLARATIONS],
//   entryComponents: [...APP_DECLARATIONS]
// }).Class({
//   constructor: function () {
//   },
//   ngDoBootstrap: () => {
//
//   }
// });
//
// platformBrowserDynamic().bootstrapModule(AppModule).then(platformRef => {
//   const upgrade = platformRef.injector.get(UpgradeModule);
//
//   angular.element(document.body).ready(function () {
//     upgrade.bootstrap(document.body, ['OrientDBStudioApp']);
//   });
//
// });




