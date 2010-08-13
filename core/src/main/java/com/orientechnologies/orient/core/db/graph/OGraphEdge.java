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
package com.orientechnologies.orient.core.db.graph;

import java.lang.ref.SoftReference;

import com.orientechnologies.orient.core.annotation.OAfterDeserialization;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * GraphDB Edge class. It represent the edge (or arc) in the graph. The Edge can have custom properties. You can read/write them
 * using respectively get/set methods. The Edge is the connection between two Vertexes.
 * 
 * @see OGraphVertex
 */
public class OGraphEdge extends OGraphElement {
	public static final String					CLASS_NAME	= "OGraphEdge";
	public static final String					IN					= "in";
	public static final String					OUT					= "out";

	private SoftReference<OGraphVertex>	in;
	private SoftReference<OGraphVertex>	out;

	public OGraphEdge(final ODatabaseGraphTx iDatabase, final ORID iRID) {
		super((ODatabaseRecord<?>) iDatabase.getUnderlying(), iRID);
	}

	public OGraphEdge(final ODatabaseRecord<?> iDatabase) {
		super(iDatabase, CLASS_NAME);
	}

	public OGraphEdge(final ODatabaseGraphTx iDatabase) {
		super((ODatabaseRecord<?>) iDatabase.getUnderlying(), CLASS_NAME);
	}

	public OGraphEdge(final ODatabaseGraphTx iDatabase, final OGraphVertex iInNode, final OGraphVertex iOutNode) {
		this((ODatabaseRecord<?>) iDatabase.getUnderlying(), iInNode, iOutNode);
	}

	public OGraphEdge(final ODatabaseRecord<?> iDatabase, final OGraphVertex iInNode, final OGraphVertex iOutNode) {
		this(iDatabase);
		in = new SoftReference<OGraphVertex>(iInNode);
		out = new SoftReference<OGraphVertex>(iOutNode);
		set(IN, iInNode.getDocument()).set(OUT, iOutNode.getDocument());
	}

	public OGraphEdge(final ODocument iDocument) {
		super(iDocument);
	}

	@OAfterDeserialization
	public void fromStream(final ODocument iDocument) {
		super.fromStream(iDocument);
		document.setTrackingChanges(false);
		in = out = null;
	}

	public OGraphVertex getIn() {
		if (in == null || in.get() == null)
			in = new SoftReference<OGraphVertex>(new OGraphVertex((ODocument) document.field(IN)));

		return in.get();
	}

	public OGraphVertex getOut() {
		if (out == null || out.get() == null)
			out = new SoftReference<OGraphVertex>(new OGraphVertex((ODocument) document.field(OUT)));

		return out.get();
	}

	protected void setIn(final OGraphVertex iSource) {
		this.in = new SoftReference<OGraphVertex>(iSource);
		document.field(IN, iSource.getDocument());
	}

	protected void setOut(final OGraphVertex iDestination) {
		this.out = new SoftReference<OGraphVertex>(iDestination);
		document.field(OUT, iDestination.getDocument());
	}

	public void delete() {
		getOut().unlink(getIn());
		getIn().unlink(getOut());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((in == null || in.get() == null) ? 0 : in.get().hashCode());
		result = prime * result + ((out == null || out.get() == null) ? 0 : out.get().hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		final OGraphEdge other = (OGraphEdge) obj;
		if (in == null || in.get() == null) {
			if (other.in != null || other.in.get() == null)
				return false;
		} else if (!in.equals(other.in))
			return false;
		if (out == null || out.get() == null) {
			if (other.out != null || other.out.get() == null)
				return false;
		} else if (!out.get().equals(other.out.get()))
			return false;
		return true;
	}
}
