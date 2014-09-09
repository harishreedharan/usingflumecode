package usingflume.ch06;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Counting Interceptor implementation. It adds a count header to each event
 * that passes through the Interceptor.
 *
 * The interceptor can be configured in flume config file as,
 * where src1 is the source
 *
 * a1.sources.src1.interceptors = interceptor1
 * a1.sources.src1.interceptors.interceptor1.type = usingflume.ch06.CounterInterceptor
 * a1.sources.src1.interceptors.interceptor1.header = count
 *
 * Default key used by the interceptor is count
 *
 */
public class CounterInterceptor implements Interceptor {
  private final String headerKey;
  private static final String CONF_HEADER_KEY = "header";
  private static final String DEFAULT_HEADER = "count";
  private final AtomicLong currentCount;

  private CounterInterceptor(Context ctx) {
    headerKey = ctx.getString(CONF_HEADER_KEY, DEFAULT_HEADER);
    currentCount = new AtomicLong(0);
  }

  @Override
  public void initialize() {
    //No op
  }

  @Override
  public Event intercept(final Event event) {
    long count = currentCount.incrementAndGet();
    event.getHeaders().put(headerKey, String.valueOf(count));
    return event;
  }

  @Override
  public List<Event> intercept(final List<Event> events) {
    for (Event e : events) {
      intercept(
        e); //ignore the return value, the event is modified in place.
    }
    return events;
  }

  @Override
  public void close() {
    // No op
  }

  public static class CounterInterceptorBuilder
    implements Interceptor.Builder {

    private Context ctx;

    @Override
    public Interceptor build() {
      return new CounterInterceptor(ctx);
    }

    @Override
    public void configure(Context context) {
      this.ctx = context;
    }
  }
}
