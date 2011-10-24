/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.type;

import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Base abstract class to wrap a document without the management of type class.
 * 
 * @see ODocumentWrapper
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings("unchecked")
public abstract class ODocumentWrapperNoClass extends ODocumentWrapper {
	public ODocumentWrapperNoClass() {
	}

	public ODocumentWrapperNoClass(final ODocument iDocument) {
		super(iDocument);
	}

	@Override
	public void fromStream(ODocument iDocument) {
		super.fromStream(iDocument);
		fromStream();
	}

	protected abstract void fromStream();

	@Override
	public <RET extends ODocumentWrapper> RET load() {
		super.load();
		fromStream();
		return (RET) this;
	}

	@Override
	public <RET extends ODocumentWrapper> RET load(final String iFetchPlan) {
		super.load(iFetchPlan);
		fromStream();
		return (RET) this;
	}

	@Override
	public <RET extends ODocumentWrapper> RET reload() {
		super.reload();
		fromStream();
		return (RET) this;
	}

	@Override
	public <RET extends ODocumentWrapper> RET reload(final String iFetchPlan) {
		super.reload(iFetchPlan);
		fromStream();
		return (RET) this;
	}

	@Override
	public <RET extends ODocumentWrapper> RET save() {
		toStream();
		super.save();
		return (RET) this;
	}

	@Override
	public <RET extends ODocumentWrapper> RET save(final String iClusterName) {
		toStream();
		super.save(iClusterName);
		return (RET) this;
	}
}
