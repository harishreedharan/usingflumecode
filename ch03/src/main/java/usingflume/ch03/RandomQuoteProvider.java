package usingflume.ch03;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class RandomQuoteProvider implements QuoteProvider {
  private final Random r = new Random(System.currentTimeMillis());

  @Override
  public void start() {
  }

  @Override
  public void stop() {
  }

  @Override
  public Map<String, Float> getQuote(List<String> tickers) {
    Map<String, Float> prices = new HashMap<String, Float>(tickers.size());
    for(String ticker: tickers) {
      prices.put(ticker, r.nextFloat());
    }
    return prices;
  }
}
