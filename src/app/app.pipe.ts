import {
  ToSizeString,
  FilterPipe,
  KeysPipe,
  EventToString
} from "./core/pipes";

export const APP_PIPES = [ToSizeString, FilterPipe, KeysPipe, EventToString];
