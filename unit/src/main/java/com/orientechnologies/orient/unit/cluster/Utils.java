package com.orientechnologies.orient.unit.cluster;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.*;

class Utils {
    private Utils(){}

    public static <T> T readFromXML(Class<T> clazz, String xmlTemplate, Object... params) throws Exception {
        final JAXBContext context = JAXBContext.newInstance(clazz);
        Unmarshaller unmarshaller = context.createUnmarshaller();
        final String xml = String.format(xmlTemplate, params);
        final InputStream is = new ByteArrayInputStream(xml.getBytes());
        return (T) unmarshaller.unmarshal(is);
    }

    public static void writeAsXmlFile(File file, Object data) throws Exception{
        assert file != null;
        assert data != null;

        final FileWriter fw = new FileWriter(file);
        try{
            Utils.writeToXml(fw, data);
        }finally{
            fw.close();
        }

    }

    public static Writer writeToXml(Writer writer, Object data) throws Exception {
        final JAXBContext context = JAXBContext.newInstance(data.getClass());
        final Marshaller marshaller = context.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
        marshaller.marshal(data, writer);
        return writer;
    }

    public static void writeAsFile(File file, String template, Object... args) throws IOException{
        final FileWriter fw = new FileWriter(file);
        try{
            fw.write(String.format(template, args));
        }finally{
            fw.close();
        }
    }
}
