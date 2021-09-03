/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

package com.orientechnologies.lucene.test;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/** Created by Enrico Risa on 22/04/15. */
public class LuceneFacetTest extends BaseLuceneTest {

  @Before
  public void init() {
    OSchema schema = db.getMetadata().getSchema();
    OClass oClass = schema.createClass("Item");

    oClass.createProperty("name", OType.STRING);
    oClass.createProperty("category", OType.STRING);

    db.command(
            new OCommandSQL(
                "create index Item.name_category on Item (name,category) FULLTEXT ENGINE LUCENE METADATA { 'facetFields' : ['category']}"))
        .execute();

    ODocument doc = new ODocument("Item");
    doc.field("name", "Pioneer");
    doc.field("category", "Electronic/HiFi");

    db.save(doc);

    doc = new ODocument("Item");
    doc.field("name", "Hitachi");
    doc.field("category", "Electronic/HiFi");

    db.save(doc);

    doc = new ODocument("Item");
    doc.field("name", "Philips");
    doc.field("category", "Electronic/HiFi");

    db.save(doc);

    doc = new ODocument("Item");
    doc.field("name", "HP");
    doc.field("category", "Electronic/Computer");

    db.save(doc);

    db.commit();
  }

  @Test
  @Ignore
  public void baseFacetTest() {

    List<ODocument> result =
        db.command(
                new OSQLSynchQuery<ODocument>(
                    "select *,$facet from Item where name lucene '(name:P*)' limit 1 "))
            .execute();

    Assert.assertEquals(result.size(), 1);

    List<ODocument> facets = result.get(0).field("$facet");

    Assert.assertEquals(facets.size(), 1);

    ODocument facet = facets.get(0);
    Assert.assertEquals(facet.<Object>field("childCount"), 1);
    Assert.assertEquals(facet.<Object>field("value"), 2);
    Assert.assertEquals(facet.field("dim"), "category");

    List<ODocument> labelsValues = facet.field("labelsValue");

    Assert.assertEquals(labelsValues.size(), 1);

    ODocument labelValues = labelsValues.get(0);

    Assert.assertEquals(labelValues.<Object>field("value"), 2);
    Assert.assertEquals(labelValues.field("label"), "Electronic");

    result =
        db.command(
                new OSQLSynchQuery<ODocument>(
                    "select *,$facet from Item where name lucene { 'q' : 'H*', 'drillDown' : 'category:Electronic' }  limit 1 "))
            .execute();

    Assert.assertEquals(result.size(), 1);

    facets = result.get(0).field("$facet");

    Assert.assertEquals(facets.size(), 1);

    facet = facets.get(0);

    Assert.assertEquals(facet.<Object>field("childCount"), 2);
    Assert.assertEquals(facet.<Object>field("value"), 2);
    Assert.assertEquals(facet.field("dim"), "category");

    labelsValues = facet.field("labelsValue");

    Assert.assertEquals(labelsValues.size(), 2);

    labelValues = labelsValues.get(0);

    Assert.assertEquals(labelValues.<Object>field("value"), 1);
    Assert.assertEquals(labelValues.field("label"), "HiFi");

    labelValues = labelsValues.get(1);

    Assert.assertEquals(labelValues.<Object>field("value"), 1);
    Assert.assertEquals(labelValues.field("label"), "Computer");
  }
}
