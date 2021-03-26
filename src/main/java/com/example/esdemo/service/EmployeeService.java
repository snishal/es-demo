package com.example.esdemo.service;

import com.example.esdemo.dto.Employee;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import javax.naming.directory.SearchResult;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
public class EmployeeService {
  @Autowired private BulkIngestService bulkIngestService;
  @Autowired private RestHighLevelClient restHighLevelClient;
  @Autowired private ObjectMapper objectMapper;

  public HttpStatus ingestData() throws IOException, InterruptedException{
    File json = new File("Employees50K.json");
    FileReader fr = new FileReader(json);
    BufferedReader br = new BufferedReader(fr);

    String line;
    List<Employee> employees = new ArrayList<>();
    Integer id = 1;
    while((line = br.readLine()) != null){
      Employee employee = objectMapper.readValue(line, Employee.class);
      employee.setId(id);
      employees.add(employee);

      id += 1;
    }
    bulkIngestService.ingestData(employees);
    return HttpStatus.OK;
  }

  public List<Employee> getAllEmployees() throws IOException{
    BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
    queryBuilder.must(QueryBuilders.matchAllQuery());

    SearchRequest request = new SearchRequest();
    request.indices("employee_index");

    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
    sourceBuilder.query(queryBuilder);
    sourceBuilder.from(0);
    sourceBuilder.size(25);

    request.source(sourceBuilder);

    SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
    SearchHit[] searchHits = response.getHits().getHits();

    List<Employee> employees = new ArrayList<>();
    for (SearchHit searchHit : searchHits){
      employees.add(objectMapper.convertValue(searchHit.getSourceAsMap(), Employee.class));
    }

    return employees;
  }

  public List<Employee> getSpecificEmployees(String searchString) throws IOException{
    BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
    boolQueryBuilder.should(QueryBuilders.termQuery("Designation", searchString));
    boolQueryBuilder.should(QueryBuilders.termQuery("MaritalStatus", searchString));
    boolQueryBuilder.should(QueryBuilders.matchQuery("FirstName", searchString).boost(0.4f)
      .fuzziness(Fuzziness.AUTO));
    boolQueryBuilder.should(QueryBuilders.matchQuery("LastName", searchString).boost(0.3f)
      .fuzziness(Fuzziness.AUTO));
    boolQueryBuilder.should(QueryBuilders.matchQuery("Interests", searchString).boost(0.4f)
      .fuzziness(Fuzziness.AUTO));

    SearchRequest searchRequest = new SearchRequest();
    searchRequest.indices("employee_index");

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(boolQueryBuilder);

    searchRequest.source(searchSourceBuilder);

    SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    SearchHit[] searchHit = searchResponse.getHits().getHits();

    List<Employee> employeeList = new ArrayList<>();
    for (SearchHit hit : searchHit) {
      employeeList.add(objectMapper.convertValue(hit.getSourceAsMap(), Employee.class));
    }

    return employeeList;
  }
}
