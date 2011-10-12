/*
 * Copyright 1999-2011 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.query.nativ;

import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Deprecated. Use OQueryContextNative class instead. Now this code:<br/>
 * <code>
 * List<ODocument> result = new ONativeSynchQuery<ODocument, OQueryContextNativeSchema<ODocument>>(database, "Profile",
 * 				new OQueryContextNativeSchema<ODocument>()) {
 * 
 * @Override public boolean filter(OQueryContextNativeSchema<ODocument> iRecord) { return
 *           iRecord.field("location").field("city").field("name").eq("Rome").and().field("name").like("G%").go(); };
 * 
 *           }.execute(); </code> can be simplified with:<br/>
 *           <code>
 * List<ODocument> result = new ONativeSynchQuery<OQueryContextNative>(database, "Profile",
 * 				new OQueryContextNative()) {
 * 
 * @Override public boolean filter(OQueryContextNative iRecord) { return
 *           iRecord.field("location").field("city").field("name").eq("Rome").and().field("name").like("G%").go(); };
 * 
 *           }.execute();
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 * @param <T>
 */
@Deprecated
public class OQueryContextNativeSchema<T extends ODocument> extends OQueryContextNative {
}
