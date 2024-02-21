import {
  DBService,
  GraphService,
  SchemaService,
  CommandService,
  ServerCommandService,
  TeleporterService,
  EtlService,
  NotificationService,
  AgentService,
  ProfilerService,
  Neo4jImporterService,
  PermissionService,
  WikiService,
  MetricService,
  BackupService,
  SecurityService,
  AuditingService,
  DistributedService
} from "./core/services";
import {
  FormatArrayPipe,
  FormatErrorPipe,
  ObjectKeysPipe,
  KeysPipe
} from "./core/pipes";

const APP_PIPES = [FormatArrayPipe, FormatErrorPipe, ObjectKeysPipe];

const APP_SERVICES = [
  DBService,
  GraphService,
  SchemaService,
  CommandService,
  ServerCommandService,
  TeleporterService,
  EtlService,
  NotificationService,
  AgentService,
  ProfilerService,
  Neo4jImporterService,
  GraphService,
  PermissionService,
  WikiService,
  MetricService,
  BackupService,
  SecurityService,
  AuditingService,
  DistributedService
];

export const APP_RESOLVER_PROVIDERS = [...APP_SERVICES, ...APP_PIPES];
