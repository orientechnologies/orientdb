package com.orientechnologies.orient.core.serialization.serializer.stream;

import java.io.IOException;

import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.OMemoryInputStream;
import com.orientechnologies.orient.core.serialization.OMemoryStream;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerStringAbstract;

/**
 * @author LomakiA <a href="mailto:Andrey.Lomakin@exigenservices.com">Andrey Lomakin</a>
 * @since 29.07.11
 */
public class OCompositeKeySerializer implements OStreamSerializer {
	public static final String									NAME			= "cks";
	public static final OCompositeKeySerializer	INSTANCE	= new OCompositeKeySerializer();

	public byte[] toStream(final Object iObject) throws IOException {
		final OCompositeKey compositeKey = (OCompositeKey) iObject;

		final OMemoryStream outputStream = new OMemoryStream();
		outputStream.set( compositeKey.getKeys().size() );
    
		for (final Comparable<?> comparable : compositeKey.getKeys()) {
      final StringBuilder builder = new StringBuilder(  );
      final OType type =  OType.getTypeByClass( comparable.getClass() );
      builder.append( type.toString());
      builder.append( "," );
      ORecordSerializerStringAbstract.fieldTypeToString(builder, type, comparable);
      
			outputStream.set( OBinaryProtocol.string2bytes( builder.toString() ));
		}

		return outputStream.toByteArray();
	}

	public Object fromStream(final byte[] iStream) throws IOException {
		final OCompositeKey compositeKey = new OCompositeKey();
		final OMemoryInputStream inputStream = new OMemoryInputStream(iStream);

		final int keysSize = inputStream.getAsInteger();
		for (int i = 0; i < keysSize; i++) {
      final byte[] keyBytes = inputStream.getAsByteArray();
      final String keyString = OBinaryProtocol.bytes2string(keyBytes);
      final int typeSeparatorPos = keyString.indexOf( ',' ); 
      final OType type = OType.valueOf( keyString.substring( 0, typeSeparatorPos ) );
      compositeKey.addKey((Comparable)ORecordSerializerStringAbstract.simpleValueFromStream( keyString.substring( typeSeparatorPos + 1 ), type ));
		}
		return compositeKey;
	}

	public String getName() {
		return NAME;
	}
}
