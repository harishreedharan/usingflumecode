/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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
  private final StockServer server = new RandomStockPriceServer();

  private volatile long lastPoll = 0;

  @Override
  protected Status doProcess() throws EventDeliveryException {
    Status status = Status.BACKOFF;
    if(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - lastPoll) >
      refreshInterval) {
      final List<Event> events = new ArrayList<Event>(tickers.size());
      Map<String, Float> prices = server.getStockPrice(tickers);
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
