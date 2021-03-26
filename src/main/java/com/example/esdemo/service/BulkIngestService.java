package com.example.esdemo.service;

import com.example.esdemo.dto.Employee;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@Slf4j
@Service
public class BulkIngestService {
  @Autowired private ObjectMapper objectMapper;
  @Autowired private RestHighLevelClient restHighLevelClient;

  public HttpStatus ingestData(List<Employee> employees) throws InterruptedException, IOException{
    BulkProcessor.Listener listener = new BulkProcessor.Listener() {
      @Override
      public void beforeBulk(long l, BulkRequest bulkRequest) {
        log.info("Ingestion Started");
      }

      @Override
      public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
        if(bulkResponse.hasFailures()){
          for(BulkItemResponse bulkItemResponse : bulkResponse){
            if(bulkItemResponse.isFailed()){
              BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
              log.error("Failed Ingestion: ", failure.getCause());
            }
          }
        }
      }

      @Override
      public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
        log.error("Error encountered: ", throwable);
      }
    };
    BulkProcessor bulkProcessor = BulkProcessor.builder(
      (request, bulkListener) -> restHighLevelClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
      listener).build();

    try{
      BulkRequest bulkRequest = new BulkRequest();

      employees.forEach(employee -> {
        Map<String, Object> map = objectMapper.convertValue(employee, HashMap.class);
        map.values().removeAll(Collections.singleton(null));

        Set<String> keys = map.keySet();
        XContentBuilder builder = null;

        try{
          builder = jsonBuilder().startObject();
          for(String key : keys){
            builder.field(key, map.get(key));
          }
          builder.endObject();
        } catch (IOException e) {
          e.printStackTrace();
        }

        IndexRequest indexRequest = new IndexRequest("employee_index", "_doc", employee.getId().toString()).source(builder);
        UpdateRequest updateRequest = new UpdateRequest("employee_index", "_doc", employee.getId().toString());
        updateRequest.doc(builder);
        updateRequest.upsert(indexRequest);

        bulkProcessor.add(updateRequest);
      });
    } catch (Exception e) {
      log.error("Error encountered: ", e);
      throw e;
    }

    try{
      boolean terminated = bulkProcessor.awaitClose(30L, TimeUnit.SECONDS);
      log.info("Ingested");
    } catch (InterruptedException e) {
      throw e;
    }

    return HttpStatus.OK;
  }
}
