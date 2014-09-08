package usingflume.ch03;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.EventDeserializerFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.List;

public class TestProtobufDeserializer {

  private EventDeserializer deserializer;
  private List<Event> events = Lists.newArrayList();
  private ByteArrayInputStream stream;

  @Before
  public void setUp() throws Exception {
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    for (int i = 0; i < 100; i++) {
      final byte[] body = ("test - " + i).getBytes();
      events.add(EventBuilder.withBody(body));
      UsingFlumeEvent.Event.Builder b = UsingFlumeEvent.Event.newBuilder();
      b.setBody(ByteString.copyFrom(body));
      UsingFlumeEvent.Event event = b.build();
      outputStream.write(ByteBuffer.allocate(Integer.SIZE/8).putInt(event
        .getSerializedSize()).array());
      event.writeTo(outputStream);
      outputStream.flush();
    }
    stream = new ByteArrayInputStream(outputStream.toByteArray());
    deserializer = EventDeserializerFactory.getInstance(
      "usingflume.ch03.ProtobufDeserializer$ProtobufDeserializerBuilder",
      new Context(),
      new ResettableProtobufInputStream(stream));
    Preconditions.checkNotNull(deserializer);
  }

  @Test
  public void testSuccessfulReads() throws Exception {
    List<Event> readEvents = deserializer.readEvents(100);
    for (int i = 0; i < 100; i++) {
      Assert.assertArrayEquals(events.get(i).getBody(), readEvents.get(i).getBody());
    }
  }

}