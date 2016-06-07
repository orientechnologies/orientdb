package com.orientechnologies.orient.object;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.object.OObjectSerializer;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import com.orientechnologies.orient.object.serialization.OObjectSerializerContext;
import com.orientechnologies.orient.object.serialization.OObjectSerializerHelper;

public class CustomDatatypeTest {

    public static class WrappedString {
	private String value;

	public String getValue() {
	    return value;
	}

	public void setValue(String value) {
	    this.value = value;
	}

	@Override
	public String toString() {
	    return "Key [value=" + getValue() + "]";
	}
    }

    public static class Entity {
	private String name;

	private WrappedString data;

	public String getName() {
	    return name;
	}

	public void setName(String name) {
	    this.name = name;
	}

	public WrappedString getData() {
	    return data;
	}

	public void setData(WrappedString value) {
	    this.data = value;
	}

	@Override
	public String toString() {
	    return "Entity [name=" + getName() + ", data=" + getData() + "]";
	}
    }

    @Test
    public void reproduce() throws Exception {
	final OObjectDatabaseTx db = new OObjectDatabaseTx(
		"memory:CustomDatatypeTest");
	db.create();

	// WrappedString custom datatype registration (storing it as
	// OType.STRING)
	OObjectSerializerContext serializerContext = new OObjectSerializerContext();
	serializerContext.bind(new OObjectSerializer<WrappedString, String>() {
	    @Override
	    public String serializeFieldValue(Class<?> iClass,
		    WrappedString iFieldValue) {
		return iFieldValue.getValue();
	    }

	    @Override
	    public WrappedString unserializeFieldValue(Class<?> iClass,
		    String iFieldValue) {
		final WrappedString result = new WrappedString();
		result.setValue(iFieldValue);
		return result;
	    }
	}, db);
	OObjectSerializerHelper.bindSerializerContext(WrappedString.class,
		serializerContext);

	// we want schema to be generated
	db.setAutomaticSchemaGeneration(true);

	// register our entity
	db.getEntityManager().registerEntityClass(Entity.class);

	// Validate DB did figure out schema properly
	Assert.assertEquals(db.getMetadata().getSchema().getClass(Entity.class)
		.getProperty("data").getType(), OType.STRING);
    }
}
