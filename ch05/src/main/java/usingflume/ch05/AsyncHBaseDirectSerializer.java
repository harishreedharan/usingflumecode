package usingflume.ch05;

import com.google.common.collect.Lists;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.AsyncHbaseEventSerializer;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.PutRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A serializer that writes data to HBase from Flume. It looks for
 * headers
 * with the following keys in the Flume event, whose values represent:
 * <p/>
 * <ul>
 * <li> rowKey - The rowkey that this event's increments or payload
 * should go
 * to</li>
 * <li> incrementColumns - A column-separated list of column names
 * whose
 * values should be incremented</li>
 * <li> payloadColumn - The column to write the event body to</li>
 * </ul>
 * <p> If the rowKey header is missing, the event is simply dropped
 * . If the
 * incrementColumns header is not present, the serializer will
 * simply not
 * increment any columns and returns an empty list as the result of
 * {@linkplain #getIncrements()}. If the payloadColumn header is
 * missing the
 * event body is not written to HBase and an empty list is returned by
 * {@linkplain #getActions()}.
 * </p>
 */

public class AsyncHBaseDirectSerializer
  implements AsyncHbaseEventSerializer {
  private byte[] table;
  private byte[] columnFamily;
  private Event currentEvent;
  private static final String ROWKEY_HEADER = "rowKey";
  private static final String INCREMENTCOLUMNS_HEADER
    = "incrementColumns";
  private static final String PAYLOADCOLUMN_HEADER = "payloadColumn";
  private final ArrayList<PutRequest> putRequests
    = Lists.newArrayList();
  private final ArrayList<AtomicIncrementRequest> incrementRequests
    = Lists
    .newArrayList();
  private byte[] currentRow;
  private String incrementColumns;
  private String payloadColumn;
  private boolean shouldProcess = false;

  @Override
  public void initialize(byte[] table, byte[] cf) {
    this.table = table;
    this.columnFamily = cf;
  }

  @Override
  public void setEvent(Event event) {
    this.currentEvent = event;
    Map<String, String> headers = currentEvent.getHeaders();
    String rowKey = headers.get(ROWKEY_HEADER);
    if (rowKey == null) {
      shouldProcess = false;
      return;
    }
    currentRow = rowKey.getBytes();
    incrementColumns = headers.get(INCREMENTCOLUMNS_HEADER);
    payloadColumn = headers.get(PAYLOADCOLUMN_HEADER);
    if (incrementColumns == null && payloadColumn == null) {
      shouldProcess = false;
      return;
    }
    shouldProcess = true;
  }

  @Override
  public List<PutRequest> getActions() {
    putRequests.clear();
    if (shouldProcess && payloadColumn != null) {
      putRequests.add(new PutRequest(table, currentRow, columnFamily,
        payloadColumn.getBytes(), currentEvent.getBody()));
    }
    return putRequests;
  }

  @Override
  public List<AtomicIncrementRequest> getIncrements() {
    incrementRequests.clear();
    if (shouldProcess && incrementColumns != null) {
      String[] incrementColumnNames = incrementColumns.split(",");
      for (String column : incrementColumnNames) {
        incrementRequests.add(
          new AtomicIncrementRequest(table, currentRow,
            columnFamily, column.getBytes()));
      }
    }
    return incrementRequests;
  }

  @Override
  public void cleanUp() {
    // Help garbage collection
    putRequests.clear();
    incrementColumns = null;
    payloadColumn = null;
    currentEvent = null;
    currentRow = null;
    table = null;
    columnFamily = null;
  }

  @Override
  public void configure(Context context) {
    // No configuration required
  }

  @Override
  public void configure(ComponentConfiguration conf) {
    // No configuration required
  }
}
