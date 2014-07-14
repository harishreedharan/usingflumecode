package usingflume.ch05;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.elasticsearch
  .AbstractElasticSearchIndexRequestBuilderFactory;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.base.Charsets;

import java.io.IOException;
import java.util.Map;

public class HeaderAndBodyIndexRequestBuilderFactory extends
  AbstractElasticSearchIndexRequestBuilderFactory {

  private String CONFIG_WRITE_HEADERS = "writeHeaders";
  // By default, don't write the headers.
  private boolean DEFAULT_WRITE_HEADERS = false;
  private boolean writeHeaders = false;
  private static final String BODY_HEADER = "body";

  public HeaderAndBodyIndexRequestBuilderFactory() {
    this(FastDateFormat.getDateInstance(FastDateFormat.FULL));
  }

  protected HeaderAndBodyIndexRequestBuilderFactory(
    FastDateFormat dateFormat) {
    super(dateFormat);
  }

  @Override
  public void configure(Context context) {
    writeHeaders = context.getBoolean(CONFIG_WRITE_HEADERS,
      DEFAULT_WRITE_HEADERS);

  }

  @Override
  public void configure(
    ComponentConfiguration componentConfiguration) {

  }

  @SuppressWarnings("unchecked")
  @Override
  protected void prepareIndexRequest(
    IndexRequestBuilder indexRequestBuilder,
    String indexName, String indexType, Event event)
    throws IOException {
    indexRequestBuilder.setIndex(indexName).setType(indexType);
    if (writeHeaders) {
      Map source = (Map) event.getHeaders();
      source.put(BODY_HEADER,
        new String(event.getBody(), Charsets.UTF_8));
      indexRequestBuilder.setSource((Map<String, Object>) source);
    } else {
      indexRequestBuilder.setSource(event.getBody());
    }

  }
}