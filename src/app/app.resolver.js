import {DBService, GraphService} from './core/services';


const APP_SERVICES = [
  DBService,
  GraphService
]

export const APP_RESOLVER_PROVIDERS = [
  ...APP_SERVICES
];
