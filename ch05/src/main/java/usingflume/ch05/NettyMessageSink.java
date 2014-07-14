package usingflume.ch05;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;


public class NettyMessageSink extends AbstractSink
  implements Configurable {

  private static final Logger LOGGER = LoggerFactory.getLogger
    (NettyMessageSink.class);
  private String hostname;
  private Integer port;
  private int batchSize;
  private static final String CONFIG_HOST = "host";
  private static final String CONFIG_PORT = "port";
  private static final String CONFIG_BATCHSIZE = "batchSize";
  private static final int DEFAULT_BATCHSIZE = 100;
  private final List<Event> eventsToSend = Lists.newArrayList();
  private int eventBodyAggregate = 0;

  @Override
  public Status process() throws EventDeliveryException {
    eventBodyAggregate = 0;
    eventsToSend.clear();
    Status status = Status.BACKOFF;
    Transaction tx = null;
    try {
      tx = getChannel().getTransaction();
      tx.begin();
      for (int i = 0; i < batchSize; i++) {
        Event e = getChannel().take();
        if (e == null) {
          break;
        }
        eventBodyAggregate += e.getBody().length;
        eventsToSend.add(e);
      }
      sendEvents(eventsToSend);
      tx.commit();
      status = Status.READY;
    } catch (Throwable th) {
      if (tx != null) {
        try {
          tx.rollback();
        } catch (Throwable th2) {
          LOGGER.warn(
            "Error while attempting to rollback transaction in " +
              "NettyMessageSink. Rollback may have failed", th2);
        }
      }
    } finally {
      if (tx != null) {
        tx.close();
      }
    }
    return status;
  }

  private void sendEvents(final List<Event> events) {
    // total size of buffer = 4 (for the total size) + 4 *
    // eventCount (total
    // of individual sizes represented as ints) + total number of
    // bytes for
    // event bodies.
    ByteBuffer buffer = ByteBuffer.allocate(events.size() * 4 +
      eventBodyAggregate);
    buffer.putInt(buffer.capacity());
    for (Event e : events) {
      buffer.putInt(e.getBody().length);
      buffer.put(e.getBody());
    }
  }

  @Override
  public void configure(Context context) {
    hostname = context.getString(CONFIG_HOST);
    Preconditions.checkNotNull(hostname,
      "Parameter host is required!");
    port = context.getInteger(CONFIG_PORT);
    Preconditions.checkNotNull(port, "Parameter port is required!");
    batchSize = context.getInteger(CONFIG_BATCHSIZE,
      DEFAULT_BATCHSIZE);
  }
}
