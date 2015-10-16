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

package com.orientechnologies.lucene.query;

import com.orientechnologies.orient.core.command.OCommandContext;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.spatial.query.SpatialArgs;

/**
 * Created by Enrico Risa on 08/01/15.
 */
public class SpatialQueryContext extends QueryContext {

    public SpatialArgs spatialArgs;

    public SpatialQueryContext(OCommandContext context, IndexSearcher searcher, Query query) {
        super(context, searcher, query);
    }

    public SpatialQueryContext(OCommandContext context, IndexSearcher searcher, Query query, Filter filter) {
        super(context, searcher, query, filter);
    }

    public SpatialQueryContext(OCommandContext context, IndexSearcher searcher, Query query, Filter filter, Sort sort) {
        super(context, searcher, query, filter, sort);
    }


    public SpatialQueryContext setSpatialArgs(SpatialArgs spatialArgs) {
        this.spatialArgs = spatialArgs;
        return this;
    }
}
