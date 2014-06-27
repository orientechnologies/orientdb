/*
 *
 *  * Copyright 2010-2014 Orient Technologies LTD (info(at)orientechnologies.com)
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

package com.orientechnologies.orient.etl.transform;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

import java.util.List;
import java.util.NoSuchElementException;

public class OCSVTransformer extends OAbstractTransformer {
  protected char         separator          = ',';
  protected boolean      columnsOnFirstLine = true;
  protected List<String> columns            = null;

  @Override
  public void configure(ODatabaseDocumentTx iDatabase, ODocument iConfiguration) {
    if (iConfiguration.containsField("separator"))
      separator = iConfiguration.field("separator").toString().charAt(0);
    if (iConfiguration.containsField("columnsOnFirstLine"))
      columnsOnFirstLine = iConfiguration.field("columnsOnFirstLine");
    if (iConfiguration.containsField("columns"))
      columns = iConfiguration.field("columns");
  }

  @Override
  public String getName() {
    return "csv2document";
  }

  @Override
  public Object next() {
    if (!hasNext())
      throw new NoSuchElementException("EOF");

    final List<String> fields = OStringSerializerHelper.smartSplit(obj.toString(), new char[] { separator }, 0, -1, false, false,
        false, false);

    if (columns == null) {
      if (!columnsOnFirstLine)
        throw new OTransformException("CSV: columnsOnFirstLine=false and no columns declared");
      columns = fields;
      return null;
    }

    final ODocument doc = new ODocument();
    for (int i = 0; i < columns.size(); ++i)
      doc.field(columns.get(i), fields.get(i));

    return doc;
  }
}
