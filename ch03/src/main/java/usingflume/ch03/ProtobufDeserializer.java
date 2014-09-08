package usingflume.ch03;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.ResettableInputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProtobufDeserializer implements EventDeserializer {

  private final ResettableInputStream stream;
  private boolean isOpen;

  private ProtobufDeserializer(ResettableInputStream stream) {
    // No configuration to do, so ignore the context.
    this.stream = stream;
    isOpen = true;
  }

  @Override
  public Event readEvent() throws IOException {
    throwIfClosed();
    // To not create an array each time or copy arrays multiple times,
    // read the data to an array that backs byte buffers,
    // then wrap that array in a stream and pass it to the Protobuf
    // parseDelimitedFrom method.
    // The format is expected to be:
    // <length of message> - int
    // <protobuf message (written using writeTo (not delimited)>
    // We assume here that the file is well-formed and the length
    // or the
    // message are not partially cut off.
    byte[] sz = new byte[4];
    if (stream.read(sz, 0, 4) != -1) {
      int length = ByteBuffer.wrap(sz).getInt();
      byte[] data = new byte[length];
      stream.read(data, 0, data.length);
      UsingFlumeEvent.Event protoEvent =
        UsingFlumeEvent.Event.parseFrom(new ByteArrayInputStream(data));
      List<UsingFlumeEvent.Header> headerList
        = protoEvent.getHeaderList();
      Map<String, String> headers = new HashMap<String, String>(
        headerList.size());
      for (UsingFlumeEvent.Header hdr : headerList) {
        headers.put(hdr.getKey(), hdr.getKey());
      }
      return EventBuilder.withBody(protoEvent.getBody().toByteArray(), headers);
    }
    return null;
  }

  @Override
  public List<Event> readEvents(int count) throws IOException {
    throwIfClosed();
    List<Event> events = new ArrayList<Event>(count);
    for (int i = 0; i < count; i++) {
      Event e = readEvent();
      if (e == null) {
        break;
      }
      events.add(e);
    }
    return events;
  }

  @Override
  public void mark() throws IOException {
    throwIfClosed();
    stream.mark();
  }

  @Override
  public void reset() throws IOException {
    throwIfClosed();
    stream.reset();
  }

  @Override
  public void close() throws IOException {
    isOpen = false;
    stream.close();
  }

  private void throwIfClosed() {
    Preconditions.checkState(isOpen, "Serializer is closed!");
  }

  public static class ProtobufDeserializerBuilder implements Builder {

    @Override
    public EventDeserializer build(Context context,
      ResettableInputStream resettableInputStream) {
      // The serializer does not need any configuration,
      // so ignore the Context instance. If some configuration has
      // to be
      // passed to the serializer this context instance can be used.
      return new ProtobufDeserializer(resettableInputStream);
    }
  }
}
