package usingflume.ch05;

import org.apache.flume.Context;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.serialization.EventSerializerFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import usingflume.ch03.UsingFlumeEvent;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public class TestProtobufSerializer {

  private ByteArrayOutputStream os;
  private EventSerializer serializer;
  private static final byte[] footer = ("End Using Flume protobuf " +
    "file").getBytes();
  private static final byte[] header = ("Begin Using Flume protobuf" +
    " file").getBytes();

  @Before
  public void setUp() {
    os = new ByteArrayOutputStream();
    Context ctx = new Context();
    ctx.put("writeHeaderAndFooter", "true");
    serializer =
      EventSerializerFactory.getInstance(
        ProtobufSerializer.Builder.class.getName(),
        ctx, os);
  }

  @Test
  public void testSerializer() throws Exception {
    serializer.afterCreate();
    final String EVENT_BASE = "test - ";
    for (int i = 0; i < 100; i++) {
      serializer.write(EventBuilder.withBody((EVENT_BASE + i).getBytes()));
    }
    serializer.beforeClose();
    serializer.flush();
    ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
    for (int i = 0; i < header.length; i++) {
      Assert.assertEquals(header[i], is.read());
    }

    for (int i = 0; i < 100; i++) {
      byte[] sz = new byte[4];
      int length = 0;
      if (is.read(sz, 0, 4) != -1) {
        length = ByteBuffer.wrap(sz).getInt();
      }
      byte[] data = new byte[length];
      is.read(data, 0, data.length);
      UsingFlumeEvent.Event e = UsingFlumeEvent.Event.parseFrom(data);
      Assert.assertArrayEquals((EVENT_BASE + i).getBytes(),
        e.getBody().toByteArray());
    }
    for (int i = 0; i < footer.length; i++) {
      Assert.assertEquals(footer[i], is.read());
    }

  }


}