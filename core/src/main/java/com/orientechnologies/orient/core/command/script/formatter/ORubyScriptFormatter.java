/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.command.script.formatter;

import java.util.Scanner;

import com.orientechnologies.orient.core.metadata.function.OFunction;

/**
 * Ruby script formatter
 * 
 * @author Luca Garulli
 * 
 */
public class ORubyScriptFormatter implements OScriptFormatter {
  public String getFunction(final OFunction f) {

    final StringBuilder fCode = new StringBuilder();
    fCode.append("def ");
    fCode.append(f.getName());
    fCode.append('(');
    int i = 0;
    for (String p : f.getParameters()) {
      if (i++ > 0)
        fCode.append(',');
      fCode.append(p);
    }
    fCode.append(")\n");

    final Scanner scanner = new Scanner(f.getCode());
    try {
      scanner.useDelimiter("\n").skip("\r");

      while (scanner.hasNext()) {
        fCode.append('\t');
        fCode.append(scanner.next());
      }
    } finally {
      scanner.close();
    }
    fCode.append("\nend\n");

    return fCode.toString();
  }
}
