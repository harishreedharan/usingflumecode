package usingflume.ch05;

import com.google.common.collect.Maps;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.sink.hbase.AsyncHbaseEventSerializer;
import org.hbase.async.PutRequest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;


public class TestAsyncHBaseDirectSerializer {

  @Test
  public void testSerializer() throws Exception {
    AsyncHbaseEventSerializer serializer = new AsyncHBaseDirectSerializer();
    serializer.initialize("testTable".getBytes(), "testCF".getBytes());
    for (int i = 0; i < 100; i++) {
      Map<String, String> headers = Maps.newHashMap();
      String key = "key - " + i;
      String payloadCol = "p";
      String incCol = "i";
      byte[] body = ("test - " + i).getBytes();
      headers.put("rowKey", key);
      headers.put("payloadColumn", payloadCol);
      headers.put("incrementColumns", incCol);
      serializer.setEvent(
        EventBuilder.withBody(body, headers));
      PutRequest request = serializer.getActions().get(0);
      Assert.assertArrayEquals(key.getBytes(), request.key());
      Assert.assertArrayEquals(payloadCol.getBytes(), request.qualifier());
      Assert.assertArrayEquals(body, request.value());
    }
  }
}
