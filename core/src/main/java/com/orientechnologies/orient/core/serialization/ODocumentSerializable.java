package com.orientechnologies.orient.core.serialization;

import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Interface for objects which are hold inside of document as field values and can serialize yourself into document. In such way it
 * is possible to serialize complex types and do not break compatibility with non-Java binary drivers.
 * 
 * After serialization into document additional field {@link #CLASS_NAME} will be added. This field contains value of class of
 * original object.
 * 
 * During deserialization of embedded object if embedded document contains {@link #CLASS_NAME} field we try to find class with given
 * name and only if this class implements {@link ODocumentSerializable} interface it will be converted to the object. So it is
 * pretty safe to use field with {@link #CLASS_NAME} in ordinary documents if it is needed.
 * 
 * Class which implements this interface should have public no-arguments constructor.
 * 
 * 
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 3/27/14
 */
public interface ODocumentSerializable {
  String CLASS_NAME = "__orientdb_serilized_class__ ";

  ODocument toDocument();

  void fromDocument(ODocument document);
}