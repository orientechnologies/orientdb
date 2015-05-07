var db = orient.getDatabase();
var iv = db.query("select name, SchemaFullAttrs.toJSON() from InventoryVertex");
var retval=[];

java.lang.System.out.println("hello, world!");

var counter=0;

for (i=0;i<iv.length;i++) 
{
  var classname = iv[i].field("name");
  retval[counter] = "create class " + classname + " extends V;";
  var sfalist = iv[i].field("SchemaFullAttrs");
  java.lang.System.out.println("sfalist = " + sfalist);
  
  if(sfalist != null) 
  {
    var sfa = JSON.parse(sfalist);
    
    delete sfa["@type"];
    delete sfa["@version"];
    var sfa_attrs = Object.keys(sfa);
    
    java.lang.System.out.println("schema full attribute list =" + sfa_attrs);
    
    for (j=0;j<sfa_attrs.length;j++)
    {
      var attr_name = sfa_attrs[j];
      java.lang.System.out.println("attribute: " + attr_name);
      counter++;
      retval[counter] = "create property " + classname + "." + attr_name + " " + sfa[attr_name] + ";";
    }
  }
  counter++;
}

print("counter = " + counter + "\n");

var ie = db.query("select distinct(name) as name from InventoryEdge");

for (i=0;i<ie.length;i++) 
{
  var index = counter + i;
  var edgeclassname = ie[i].field("name");
  retval[index] = "create class " + edgeclassname + " extends E;";
  
  var sfalist_e = ie[i].field("SchemaFullAttrs");
  java.lang.System.out.println("sfalist = " + sfalist_e);
  
  if(sfalist_e != null) 
  {
    var sfa_e = JSON.parse(sfalist_e);
    
    delete sfa_e["@type"];
    delete sfa_e["@version"];
    var sfa_attrs_e = Object.keys(sfa_e);
    
    java.lang.System.out.println("schema full attribute list =" + sfa_attrs_e);
    
    for (j=0;j<sfa_attrs_e.length;j++)
    {
      var attr_name_e = sfa_attrs_e[j];
      java.lang.System.out.println("attribute: " + attr_name_e);
      counter++;
      retval[index] = "create property " + edgeclassname + "." + attr_name_e + " " + sfa_e[attr_name_e] + ";";
    }
  }
  index++;
}


return retval;
