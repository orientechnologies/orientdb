package com.orientechnologies.orient.core.serialization.serializer.record.string;

import com.orientechnologies.orient.core.metadata.schema.OType;
import java.util.HashMap;
import java.util.Map;

public class OFieldTypesString {

  public static final String ATTRIBUTE_FIELD_TYPES = "@fieldTypes";

  /**
   * Parses the field type char returning the closer type. Default is STRING. b=binary if
   * iValue.length() >= 4 b=byte if iValue.length() <= 3 s=short, l=long f=float d=double a=date
   * t=datetime
   *
   * @param iValue Value to parse
   * @param iCharType Char value indicating the type
   * @return The closest type recognized
   */
  public static OType getType(final String iValue, final char iCharType) {
    if (iCharType == 'f') return OType.FLOAT;
    else if (iCharType == 'c') return OType.DECIMAL;
    else if (iCharType == 'l') return OType.LONG;
    else if (iCharType == 'd') return OType.DOUBLE;
    else if (iCharType == 'b') {
      if (iValue.length() >= 1 && iValue.length() <= 3) return OType.BYTE;
      else return OType.BINARY;
    } else if (iCharType == 'a') return OType.DATE;
    else if (iCharType == 't') return OType.DATETIME;
    else if (iCharType == 's') return OType.SHORT;
    else if (iCharType == 'e') return OType.EMBEDDEDSET;
    else if (iCharType == 'g') return OType.LINKBAG;
    else if (iCharType == 'z') return OType.LINKLIST;
    else if (iCharType == 'm') return OType.LINKMAP;
    else if (iCharType == 'x') return OType.LINK;
    else if (iCharType == 'n') return OType.LINKSET;
    else if (iCharType == 'u') return OType.CUSTOM;

    return OType.STRING;
  }

  public static OType getOTypeFromChar(final char iCharType) {
    if (iCharType == 'f') return OType.FLOAT;
    else if (iCharType == 'c') return OType.DECIMAL;
    else if (iCharType == 'l') return OType.LONG;
    else if (iCharType == 'd') return OType.DOUBLE;
    else if (iCharType == 'b') return OType.BINARY;
    else if (iCharType == 'a') return OType.DATE;
    else if (iCharType == 't') return OType.DATETIME;
    else if (iCharType == 's') return OType.SHORT;
    else if (iCharType == 'e') return OType.EMBEDDEDSET;
    else if (iCharType == 'g') return OType.LINKBAG;
    else if (iCharType == 'z') return OType.LINKLIST;
    else if (iCharType == 'm') return OType.LINKMAP;
    else if (iCharType == 'x') return OType.LINK;
    else if (iCharType == 'n') return OType.LINKSET;
    else if (iCharType == 'u') return OType.CUSTOM;

    return OType.STRING;
  }

  public static Map<String, Character> loadFieldTypesV0(
      Map<String, Character> fieldTypes, final String fieldValueAsString) {
    // LOAD THE FIELD TYPE MAP
    final String[] fieldTypesParts = fieldValueAsString.split(",");
    if (fieldTypesParts.length > 0) {
      if (fieldTypes == null) {
        fieldTypes = new HashMap<>();
      }
      String[] part;
      for (String f : fieldTypesParts) {
        part = f.split("=");
        if (part.length == 2) fieldTypes.put(part[0], part[1].charAt(0));
      }
    }
    return fieldTypes;
  }

  public static Map<String, Character> loadFieldTypes(final String fieldValueAsString) {
    Map<String, Character> fieldTypes = new HashMap<>();
    loadFieldTypesV0(fieldTypes, fieldValueAsString);
    return fieldTypes;
  }
}
