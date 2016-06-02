/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.server.network.protocol.http.command.post;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerCSVAbstract;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandDocumentAbstract;

import java.io.BufferedReader;
import java.io.StringReader;
import java.text.NumberFormat;
import java.util.InputMismatchException;
import java.util.List;
import java.util.Locale;

public class OServerCommandPostImportRecords extends OServerCommandDocumentAbstract {
  private static final char     CSV_SEPARATOR     = ',';
  private static final char     CSV_STR_DELIMITER = '"';

  private static final String[] NAMES             = { "POST|importRecords/*" };

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] urlParts = checkSyntax(iRequest.url, 4,
        "Syntax error: importRecords/<database>/<format>/<class>[/<separator>][/<string-delimiter>][/<locale>]");

    final long start = System.currentTimeMillis();

    iRequest.data.commandInfo = "Import records";

    ODatabaseDocument db = getProfiledDatabaseInstance(iRequest);
    try {

      final OClass cls = db.getMetadata().getSchema().getClass(urlParts[3]);
      if (cls == null)
        throw new IllegalArgumentException("Class '" + urlParts[3] + " is not defined");

      if (iRequest.content == null)
        throw new IllegalArgumentException("Empty content");

      if (urlParts[2].equalsIgnoreCase("csv")) {
        final char separator = urlParts.length > 4 ? urlParts[4].charAt(0) : CSV_SEPARATOR;
        final char stringDelimiter = urlParts.length > 5 ? urlParts[5].charAt(0) : CSV_STR_DELIMITER;
        final Locale locale = urlParts.length > 6 ? new Locale(urlParts[6]) : Locale.getDefault();

        final BufferedReader reader = new BufferedReader(new StringReader(iRequest.content));
        String header = reader.readLine();
        if (header == null || (header = header.trim()).length() == 0)
          throw new InputMismatchException("Missing CSV file header");

        final List<String> columns = OStringSerializerHelper.smartSplit(header, separator);
        for (int i = 0; i < columns.size(); ++i)
          columns.set(i, OIOUtils.getStringContent(columns.get(i)));

        int imported = 0;
        int errors = 0;

        final StringBuilder output = new StringBuilder(1024);

        int line = 0;
        int col = 0;
        String column = "?";
        String parsedCell = "?";
        final NumberFormat numberFormat = NumberFormat.getNumberInstance(locale);

        for (line = 2; reader.ready(); line++) {
          try {
            final String parsedRow = reader.readLine();
            if (parsedRow == null)
              break;

            final ODocument doc = new ODocument(cls);
            final String row = parsedRow.trim();
            final List<String> cells = OStringSerializerHelper.smartSplit(row, CSV_SEPARATOR);

            for (col = 0; col < columns.size(); ++col) {
              parsedCell = cells.get(col);
              column = columns.get(col);

              String cellValue = parsedCell.trim();

              if (cellValue.length() == 0 || cellValue.equalsIgnoreCase("null"))
                continue;

              Object value;
              if (cellValue.length() >= 2 && cellValue.charAt(0) == stringDelimiter
                  && cellValue.charAt(cellValue.length() - 1) == stringDelimiter)
                value = OIOUtils.getStringContent(cellValue);
              else {
                try {
                  value = numberFormat.parse(cellValue);
                } catch (Exception e) {
                  value = ORecordSerializerCSVAbstract.getTypeValue(cellValue);
                }
              }

              doc.field(columns.get(col), value);
            }

            doc.save();
            imported++;

          } catch (Exception e) {
            errors++;
            output.append(String.format("#%d: line %d column %s (%d) value '%s': '%s'\n", errors, line, column, col, parsedCell,
                e.toString()));
          }
        }

        final float elapsed = (System.currentTimeMillis() - start) / 1000;

        String message = String
            .format(
                "Import of records of class '%s' completed in %5.3f seconds. Line parsed: %d, imported: %d, error: %d\nDetailed messages:\n%s",
                cls.getName(), elapsed, line, imported, errors, output);

        iResponse.send(OHttpUtils.STATUS_CREATED_CODE, OHttpUtils.STATUS_CREATED_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN,
            message, null);
        return false;

      } else
        throw new UnsupportedOperationException("Unsupported format on importing record. Available formats are: csv");

    } finally {
      if (db != null)
        db.close();
    }
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
