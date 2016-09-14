/*
 *
 *  * Copyright 2014 Orient Technologies.
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

package com.orientechnologies.lucene.tx;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.lucene.engine.OLuceneIndexEngine;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;

/**
 * Created by Enrico Risa on 28/09/15.
 */
public abstract class OLuceneTxChangesAbstract implements OLuceneTxChanges {

  public static final String TMP = "_tmp_rid";

  protected final IndexWriter        writer;
  protected final OLuceneIndexEngine engine;
  protected final IndexWriter        deletedIdx;

  public OLuceneTxChangesAbstract(OLuceneIndexEngine engine, IndexWriter writer, IndexWriter deletedIdx) {
    this.writer = writer;
    this.engine = engine;
    this.deletedIdx = deletedIdx;
  }

  public IndexSearcher searcher() {
    // TODO optimize
    try {
      return new IndexSearcher(DirectoryReader.open(writer, true, true));
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error during searcher instantiation", e);
    }

    return null;
  }

  @Override
  public long deletedDocs(Query query) {

    try {
      IndexSearcher indexSearcher = new IndexSearcher(DirectoryReader.open(deletedIdx, true, true));

      //      if (filter != null) {
      //        TopDocs search = indexSearcher.search(query, filter, Integer.MAX_VALUE);
      //        return search.totalHits;
      //      } else {
      TopDocs search = indexSearcher.search(query, Integer.MAX_VALUE);
      return search.totalHits;
      //      }
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error during searcher instantiation", e);
    }

    return 0;
  }
}
