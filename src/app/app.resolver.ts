import {DBService, GraphService, SchemaService, CommandService, TeleporterService, NotificationService} from './core/services';
import {FormatArrayPipe, FormatErrorPipe} from './core/pipes';

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
  NotificationService,
  GraphService
]


export const APP_RESOLVER_PROVIDERS = [
  ...APP_SERVICES,
  ...APP_PIPES
];
