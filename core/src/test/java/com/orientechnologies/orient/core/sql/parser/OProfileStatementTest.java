package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class OProfileStatementTest extends OParserTestAbstract{


  @Test
  public void test() {
    checkRightSyntax("profile select from V");
    checkRightSyntax("profile MATCH {as:v, class:V} RETURN $elements");
    checkRightSyntax("profile UPDATE V SET name = 'foo'");
    checkRightSyntax("profile INSERT INTO V SET name = 'foo'");
    checkRightSyntax("profile DELETE FROM Foo");
  }


}
