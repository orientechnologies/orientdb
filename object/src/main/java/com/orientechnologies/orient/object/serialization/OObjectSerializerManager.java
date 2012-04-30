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
package com.orientechnologies.orient.object.serialization;

import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.entity.OEntityManager;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.object.OObjectSerializerHelperInterface;

/**
 * @author luca.molino
 * 
 */
public class OObjectSerializerManager implements OObjectSerializerHelperInterface {

	private static final OObjectSerializerManager	instance	= new OObjectSerializerManager();

	public ODocument toStream(Object iPojo, ODocument iRecord, OEntityManager iEntityManager, OClass schemaClass,
			OUserObject2RecordHandler iObj2RecHandler, ODatabaseObject db, boolean iSaveOnlyDirty) {
		return OObjectSerializerHelper.toStream(iPojo, iRecord, iEntityManager, schemaClass, iObj2RecHandler, db, iSaveOnlyDirty);
	}

	public String getDocumentBoundField(Class<?> iClass) {
		return OObjectSerializerHelper.getDocumentBoundField(iClass);
	}

	public Object getFieldValue(Object iPojo, String iProperty) {
		return OObjectSerializerHelper.getFieldValue(iPojo, iProperty);
	}

	public void invokeCallback(Object iPojo, ODocument iDocument, Class<?> iAnnotation) {
		OObjectSerializerHelper.invokeCallback(iPojo, iDocument, iAnnotation);
	}

	public static OObjectSerializerManager getInstance() {
		return instance;
	}

}
