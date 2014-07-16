package usingflume.ch03;

import com.google.common.base.Preconditions;
import org.apache.commons.codec.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractPollableSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class StockTickerSource extends AbstractPollableSource {

  private static final String CONF_TICKERS = "tickers";
  private static final String CONF_REFRESH_INTERVAL = "refreshInterval";
  private static final int DEFAULT_REFRESH_INTERVAL = 10;

  private int refreshInterval = DEFAULT_REFRESH_INTERVAL;

  private final List<String> tickers = new ArrayList<String>();
  private final QuoteProvider server = new RandomQuoteProvider();

  private volatile long lastPoll = 0;

  @Override
  protected Status doProcess() throws EventDeliveryException {
    Status status = Status.BACKOFF;
    if(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - lastPoll) >
      refreshInterval) {
      final List<Event> events = new ArrayList<Event>(tickers.size());
      Map<String, Float> prices = server.getQuote(tickers);
      lastPoll = System.currentTimeMillis();
      // Convert each price into ticker = price form in UTF-8 as event body
      for(Map.Entry<String, Float> e: prices.entrySet()) {
        StringBuilder builder = new StringBuilder(e.getKey());
        builder.append(" = ").append(e.getValue());
        events.add(EventBuilder.withBody(builder.toString().getBytes(Charsets
          .UTF_8)));
      }
      getChannelProcessor().processEventBatch(events);
      status = Status.READY;
    }
    return status;
  }

  @Override
  protected void doConfigure(Context context) throws FlumeException {
    refreshInterval = context.getInteger(CONF_REFRESH_INTERVAL,
      DEFAULT_REFRESH_INTERVAL);
    String tickersString = context.getString(CONF_TICKERS);
    Preconditions.checkArgument(tickersString != null && !tickersString
      .isEmpty(), "A list of tickers must be specified");
    tickers.addAll(Arrays.asList(tickersString.split("\\s+")));
  }

  @Override
  protected void doStart() throws FlumeException {
    server.start();
  }

  @Override
  protected void doStop() throws FlumeException {
    server.stop();
  }
}
