package com.orientechnologies.orient.core.serialization.serializer;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.common.io.OIOUtils;

import java.util.ArrayList;
import java.util.List;

public class OStringSerializerHelperTest {

    @Test
    public void test(){
        final List<String> stringItems = new ArrayList<String>();
        final String text = "['f\\\'oo', 'don\\\'t can\\\'t', \"\\\"bar\\\"\", 'b\\\"a\\\'z', \"q\\\"u\\'x\"]";
        final int startPos = 0;

        OStringSerializerHelper.getCollection(text, startPos, stringItems,
                OStringSerializerHelper.LIST_BEGIN, OStringSerializerHelper.LIST_END, OStringSerializerHelper.COLLECTION_SEPARATOR);

        Assert.assertEquals(OIOUtils.getStringContent(stringItems.get(0)), "f'oo");
        Assert.assertEquals(OIOUtils.getStringContent(stringItems.get(1)), "don't can't");
        Assert.assertEquals(OIOUtils.getStringContent(stringItems.get(2)), "\"bar\"");
        Assert.assertEquals(OIOUtils.getStringContent(stringItems.get(3)), "b\"a\'z");
        Assert.assertEquals(OIOUtils.getStringContent(stringItems.get(4)), "q\"u\'x");
    }
}
