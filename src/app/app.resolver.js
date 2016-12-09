import {DBService} from './core/services';


const APP_SERVICES = [
  DBService
]

export const APP_RESOLVER_PROVIDERS = [
  ...APP_SERVICES
];
