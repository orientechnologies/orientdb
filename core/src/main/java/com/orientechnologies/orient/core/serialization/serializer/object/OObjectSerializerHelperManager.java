/*
 *
 * Copyright 2012 Luca Molino (molino.luca--AT--gmail.com)
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
package com.orientechnologies.orient.core.serialization.serializer.object;

import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.entity.OEntityManager;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Manager class that manages common operations against objects
 * 
 * @author luca.molino
 * 
 */
public class OObjectSerializerHelperManager {

	private static final OObjectSerializerHelperManager	instance					= new OObjectSerializerHelperManager();

	private OObjectSerializerHelperInterface						serializerHelper	= new OObjectSerializerHelperDocument();

	public static OObjectSerializerHelperManager getInstance() {
		return instance;
	}

	public ODocument toStream(final Object iPojo, final ODocument iRecord, final OEntityManager iEntityManager,
			final OClass schemaClass, final OUserObject2RecordHandler iObj2RecHandler, final ODatabaseObject db,
			final boolean iSaveOnlyDirty) {
		return serializerHelper.toStream(iPojo, iRecord, iEntityManager, schemaClass, iObj2RecHandler, db, iSaveOnlyDirty);
	}

	public String getDocumentBoundField(final Class<?> iClass) {
		return serializerHelper.getDocumentBoundField(iClass);
	}

	public Object getFieldValue(final Object iPojo, final String iProperty) {
		return serializerHelper.getFieldValue(iPojo, iProperty);
	}

	public void invokeCallback(final Object iPojo, final ODocument iDocument, final Class<?> iAnnotation) {
		serializerHelper.invokeCallback(iPojo, iDocument, iAnnotation);
	}

	public void registerHelper(OObjectSerializerHelperInterface iSerializerHelper) {
		serializerHelper = iSerializerHelper;
	}

}
