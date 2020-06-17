/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** @author richter */
public class CompositeException extends RuntimeException {
  private static final long serialVersionUID = 1L;
  private final List<Throwable> causes = new ArrayList<Throwable>();

  public CompositeException(Collection<? extends Throwable> causes) {
    this.causes.addAll(causes);
  }

  @SuppressWarnings("CallToPrintStackTrace")
  @Override
  public void printStackTrace() {
    if (causes.isEmpty()) {
      super.printStackTrace();
      return;
    }
    for (Throwable cause : causes) {
      cause.printStackTrace();
    }
  }

  @Override
  public void printStackTrace(PrintStream s) {
    if (causes.isEmpty()) {
      super.printStackTrace(s);
    } else {
      for (Throwable cause : causes) {
        cause.printStackTrace(s);
      }
    }
  }

  @Override
  public void printStackTrace(PrintWriter s) {
    if (causes.isEmpty()) {
      super.printStackTrace(s);
    } else {
      for (Throwable cause : causes) {
        cause.printStackTrace(s);
      }
    }
  }
}
