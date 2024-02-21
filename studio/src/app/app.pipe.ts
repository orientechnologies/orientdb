import {
  ToSizeString,
  FilterPipe,
  KeysPipe,
  EventToString,
  OperationPipe
} from "./core/pipes";
import { TranslationPipe } from "./core/pipes/translation.pipe";

export const APP_PIPES = [
  ToSizeString,
  FilterPipe,
  KeysPipe,
  EventToString,
  OperationPipe,
  TranslationPipe
];
