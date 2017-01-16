import {DBService, GraphService, SchemaService, CommandService} from './core/services';
import {FormatArrayPipe, FormatErrorPipe} from './core/pipes';
import {DBService, GraphService, TeleporterService} from './core/services';

const APP_PIPES = [
  FormatArrayPipe,
  FormatErrorPipe
]

const APP_SERVICES = [
  DBService,
  GraphService,
  SchemaService,
  CommandService,
  TeleporterService,
  GraphService
]


export const APP_RESOLVER_PROVIDERS = [
  ...APP_SERVICES,
  ...APP_PIPES
];
