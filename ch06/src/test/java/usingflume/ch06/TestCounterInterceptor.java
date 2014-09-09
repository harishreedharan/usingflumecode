package usingflume.ch06;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestCounterInterceptor {

  private Interceptor interceptor;

  @Before
  public void setUp() {
    Context ctx = new Context();
    ctx.put("header", "eventCount");
    final CounterInterceptor.CounterInterceptorBuilder builder = new
      CounterInterceptor.CounterInterceptorBuilder();
    builder.configure(ctx);
    interceptor = builder.build();
  }

  @Test
  public void testInterceptor() {
    List<Event> eventList = Lists.newArrayList();
    final String EVENT_BASE = "test - ";
    for (int i = 0; i < 100; i++) {
      eventList.add(EventBuilder.withBody((EVENT_BASE + i).getBytes()));
    }
    List<Event> eventsOut = interceptor.intercept(eventList);
    for (int i = 0; i < 100; i++) {
      Assert.assertEquals(Long.valueOf(i + 1),
        Long.valueOf(eventsOut.get(i).getHeaders().get("eventCount")));
      Assert.assertArrayEquals((EVENT_BASE + i).getBytes(),
        eventsOut.get(i).getBody());
    }
  }

  @Test
  public void testInterceptorMultiThreaded() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(5);
    final Queue<Event> processed = new ConcurrentLinkedQueue<Event>();
    for (int i = 0; i < 5; i++) {
      executor.submit(new Runnable() {
        @Override
        public void run() {
          List<Event> eventList = Lists.newArrayList();
          final String EVENT_BASE = "test - ";
          for (int i = 0; i < 100; i++) {
            eventList.add(EventBuilder.withBody((EVENT_BASE + i).getBytes()));
          }
          processed.addAll(interceptor.intercept(eventList));
        }
      });
    }
    Thread.sleep(2000);
    Assert.assertEquals(500, processed.size());
    Set<Integer> counts = Sets.newTreeSet();
    Iterator<Event> iter = processed.iterator();
    while(iter.hasNext()) {
      Event t = iter.next();
      counts.add(Integer.valueOf(t.getHeaders().get
        ("eventCount")));

    }
    Assert.assertEquals(500, counts.size());
    Iterator<Integer> counterIter = counts.iterator();
    Integer expected = 1;
    while(counterIter.hasNext()) {
      Integer count = counterIter.next();
      Assert.assertEquals((Integer)expected++, count);
    }

  }
}