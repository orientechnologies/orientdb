import {DBService, GraphService, SchemaService, CommandService, TeleporterService, NotificationService,
        AgentService, ProfilerService, Neo4jImporterService} from './core/services';
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
  Neo4jImporterService,
  GraphService
]


export const APP_RESOLVER_PROVIDERS = [
  ...APP_SERVICES,
  ...APP_PIPES
];
