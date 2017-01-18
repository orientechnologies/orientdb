/*
 *
 * Copyright 2012 Luca Molino (molino.luca--AT--gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.exception;

/**
 * Exception which is thrown by {@link com.orientechnologies.orient.core.index.OIndexChangesWrapper}
 * if index which is related to wrapped cursor is being rebuilt.
 *
 * @see com.orientechnologies.orient.core.index.OIndexAbstract#getRebuildVersion()
 */
public class OIndexIsRebuildingException extends ORetryQueryException {
  public OIndexIsRebuildingException(OIndexIsRebuildingException exception) {
    super(exception);
  }

  public OIndexIsRebuildingException(String message) {
    super(message);
  }
}
