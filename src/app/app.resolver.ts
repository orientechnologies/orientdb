import {DBService, GraphService, SchemaService, CommandService, TeleporterService, NotificationService,
        AgentService, ProfilerService} from './core/services';
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
  AgentService,
  ProfilerService,
  GraphService
]


export const APP_RESOLVER_PROVIDERS = [
  ...APP_SERVICES,
  ...APP_PIPES
];
