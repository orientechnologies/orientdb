package com.orientechnologies.orient.core.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.type.ODocumentWrapperNoClass;

public class OSimpleKeyIndexDefinition extends ODocumentWrapperNoClass implements OIndexDefinition {
	private OType[]	keyTypes;

	public OSimpleKeyIndexDefinition(final OType... keyTypes) {
		super(new ODocument());
		this.keyTypes = keyTypes;
	}

	public OSimpleKeyIndexDefinition() {
	}

	public List<String> getFields() {
		return Collections.emptyList();
	}

	public String getClassName() {
		return null;
	}

	public Comparable<?> createValue(final List<?> params) {
		if (params == null || params.isEmpty())
			return null;

		if (keyTypes.length == 1)
			return (Comparable<?>) OType.convert(params.iterator().next(), keyTypes[0].getDefaultJavaType());

		final OCompositeKey compositeKey = new OCompositeKey();

		int i = 0;
		for (final Object param : params) {
			final Comparable<?> paramValue = (Comparable<?>) OType.convert(param, keyTypes[i].getDefaultJavaType());

			if (paramValue == null)
				return null;
			compositeKey.addKey(paramValue);
			i++;
		}

		return compositeKey;
	}

	public Comparable<?> createValue(final Object... params) {
		return createValue(Arrays.asList(params));
	}

	public int getParamCount() {
		return keyTypes.length;
	}

	public OType[] getTypes() {
		return keyTypes;
	}

	@Override
	public ODocument toStream() {
		document.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);
		try {

			final List<String> keyTypeNames = new ArrayList<String>(keyTypes.length);

			for (final OType keyType : keyTypes)
				keyTypeNames.add(keyType.toString());

			document.field("keyTypes", keyTypeNames, OType.EMBEDDEDLIST);
			return document;
		} finally {
			document.setInternalStatus(ORecordElement.STATUS.LOADED);
		}
	}

	@Override
	protected void fromStream() {
		final List<String> keyTypeNames = document.field("keyTypes");
		keyTypes = new OType[keyTypeNames.size()];

		int i = 0;
		for (final String keyTypeName : keyTypeNames) {
			keyTypes[i] = OType.valueOf(keyTypeName);
			i++;
		}
	}

	public Object getDocumentValueToIndex(final ODocument iDocument) {
		throw new OIndexException("This method is not supported in given index definition.");
	}

	@Override
	public boolean equals(final Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		final OSimpleKeyIndexDefinition that = (OSimpleKeyIndexDefinition) o;
		if (!Arrays.equals(keyTypes, that.keyTypes))
			return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + (keyTypes != null ? Arrays.hashCode(keyTypes) : 0);
		return result;
	}

	@Override
	public String toString() {
		return "OSimpleKeyIndexDefinition{" + "keyTypes=" + (keyTypes == null ? null : Arrays.asList(keyTypes)) + '}';
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @param indexName
	 * @param indexType
	 */
	public String toCreateIndexDDL(final String indexName, final String indexType) {
		final StringBuilder ddl = new StringBuilder("create index ");
		ddl.append(indexName).append(" ").append(indexType).append(" ");

		if (keyTypes != null && keyTypes.length > 0) {
			ddl.append(keyTypes[0].toString());
			for (int i = 1; i < keyTypes.length; i++) {
				ddl.append(", ").append(keyTypes[i].toString());
			}
		}
		return ddl.toString();
	}
}
