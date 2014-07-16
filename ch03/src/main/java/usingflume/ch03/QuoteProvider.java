package usingflume.ch03;

import java.util.List;
import java.util.Map;

public interface QuoteProvider {

  void start();

  void stop();
  /**
   * Returns a map of ticker to its current price.
   * @param tickers - List of tickers whose prices must be fetched.
   * @return - a map of ticker to its current price
   */
  Map<String, Float> getQuote(List<String> tickers);
}
