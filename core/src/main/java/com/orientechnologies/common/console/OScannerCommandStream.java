/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.common.console;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

/** @author Artem Orobets (enisher-at-gmail.com) */
public class OScannerCommandStream implements OCommandStream {
  private Scanner scanner;

  public OScannerCommandStream(String commands) {
    scanner = new Scanner(commands);
    init();
  }

  public OScannerCommandStream(File file) throws FileNotFoundException {
    scanner = new Scanner(file);
    init();
  }

  private void init() {
    scanner.useDelimiter(";(?=([^\"]*\"[^\"]*\")*[^\"]*$)(?=([^']*'[^']*')*[^']*$)|\n");
  }

  @Override
  public boolean hasNext() {
    return scanner.hasNext();
  }

  @Override
  public String nextCommand() {
    return scanner.next().trim();
  }

  @Override
  public void close() {
    scanner.close();
  }
}
