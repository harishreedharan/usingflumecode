package usingflume.ch03;

import com.google.common.collect.Lists;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractPollableSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RandomGeneratingPollableSource
  extends AbstractPollableSource {
  private static final Logger LOG = LoggerFactory.getLogger
    (RandomGeneratingPollableSource.class);

  private int backoffCounter = 100;
  private int count = 0;
  private boolean backoffThisTime = false;


  @Override
  protected Status doProcess() throws EventDeliveryException {
    List<Event> events = Lists.newArrayList();
    if (!backoffThisTime) {
      for (int i = 0; i < backoffCounter; i++) {
        events.add(EventBuilder.withBody(
          RandomStringUtils.randomAlphanumeric(10).getBytes()));
      }
      getChannelProcessor().processEventBatch(events);
      backoffThisTime = true;
      return Status.READY;
    }
    backoffThisTime = false;
    return Status.BACKOFF;
  }

  @Override
  protected void doConfigure(Context context) throws FlumeException {
    backoffCounter = context.getInteger("count", 100);
  }

  @Override
  protected void doStart() throws FlumeException {
    LOG.info(
      "Starting Random Generating Pollable Source, " + getName() +
        " with backoffCounter = " + backoffCounter);
  }

  @Override
  protected void doStop() throws FlumeException {
    LOG.info(
      "Stopping Random Generating Pollable Source, " + getName() +
        ".");

  }
}
