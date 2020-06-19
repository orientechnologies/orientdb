package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.record.ODirection;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import org.apache.tinkerpop.gremlin.structure.Direction;

public class OrientGraphUtils {
  public static final String CONNECTION_OUT = "out";
  public static final String CONNECTION_IN = "in";

  public static String encodeClassName(String iClassName) {
    if (iClassName == null) return null;

    if (Character.isDigit(iClassName.charAt(0))) iClassName = "-" + iClassName;

    try {
      return URLEncoder.encode(iClassName, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      return iClassName;
    }
  }

  public static String decodeClassName(String iClassName) {
    if (iClassName == null) return null;

    if (iClassName.charAt(0) == '-') iClassName = iClassName.substring(1);

    try {
      return URLDecoder.decode(iClassName, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      return iClassName;
    }
  }

  public static ODirection mapDirection(Direction direction) {

    switch (direction) {
      case OUT:
        return ODirection.OUT;
      case IN:
        return ODirection.IN;
      case BOTH:
        return ODirection.BOTH;
    }
    throw new IllegalArgumentException(
        String.format("Cannot get direction for argument %s", direction));
  }
}
