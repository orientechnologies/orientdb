/*
 * Copyright 2016 OrientDB.
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
package com.orientechnologies.orient.core.query.live;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 *
 * @author saltuk
 */
public class ORecordLiveOperation extends ORecordOperation {

    public final OIdentifiable before;

    public ORecordLiveOperation(OIdentifiable before, OIdentifiable current, byte iStatus) {
        super(current, iStatus);
        if (before != null) {
            final ODocument doc  =before.getRecord();
            doc.setInternalStatus(ORecordElement.STATUS.LOADED);
            
            this.before = doc;
        } else {
            this.before = null;
        }
    }

}
