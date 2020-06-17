/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (info(-at-)orientdb.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.orient.etl.extractor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.OETLExtractedItem;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.NoSuchElementException;

public class OETLRowExtractor extends OETLAbstractSourceExtractor {
  protected BufferedReader bReader;
  protected OETLExtractedItem next;
  protected boolean multiLine = true;
  protected String lineFeed = "\r\n";

  @Override
  public String getName() {
    return "row";
  }

  @Override
  public void configure(final ODocument iConfiguration, final OCommandContext iContext) {
    super.configure(iConfiguration, iContext);

    if (iConfiguration.containsField("multiLine"))
      multiLine = iConfiguration.<Boolean>field("multiLine");

    if (iConfiguration.containsField("lineFeed")) lineFeed = iConfiguration.field("lineFeed");
  }

  @Override
  public boolean hasNext() {
    if (next != null) return true;

    if (bReader == null) return false;

    try {
      next = fetchNext();
      return next != null;
    } catch (IOException e) {
      throw new OETLExtractorException(e);
    }
  }

  @Override
  public OETLExtractedItem next() {
    if (next != null) {
      final OETLExtractedItem ret = next;
      next = null;
      return ret;
    }

    if (!hasNext()) throw new NoSuchElementException("EOF");

    try {
      return fetchNext();
    } catch (IOException e) {
      throw new OETLExtractorException(e);
    }
  }

  @Override
  public void extract(final Reader iReader) {
    super.extract(iReader);
    bReader = new BufferedReader(reader);
  }

  @Override
  public void end() {
    if (bReader != null)
      try {
        bReader.close();
      } catch (IOException e) {
      }

    super.end();
  }

  @Override
  public String getUnit() {
    return "rows";
  }

  protected OETLExtractedItem fetchNext() throws IOException {
    if (!bReader.ready()) return null;

    final String line = readLine();

    if (line == null || line.isEmpty()) return null;

    return new OETLExtractedItem(current++, line);
  }

  protected String readLine() throws IOException {
    if (multiLine) {
      // CONSIDER MULTIPLE LINES
      final StringBuilder sbLine = new StringBuilder();
      boolean isOpenQuote = false;
      do {
        if (isOpenQuote) {
          sbLine.append(lineFeed);
        }

        final String l = bReader.readLine();
        if (l == null) break;

        sbLine.append(l);

        // CHECK FOR OPEN QUOTE
        for (char c : l.toCharArray()) if ('"' == c) isOpenQuote = !isOpenQuote;

      } while (isOpenQuote);

      return sbLine.toString();
    }

    return bReader.readLine();
  }
}
