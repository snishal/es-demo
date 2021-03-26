package com.example.esdemo.controller;

import com.example.esdemo.dto.Employee;
import com.example.esdemo.service.EmployeeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;

@RestController
@RequestMapping("/employee")
public class EmployeeController {
  @Autowired private EmployeeService employeeService;

  @PostMapping("/ingest")
  public HttpStatus ingestData() throws IOException, InterruptedException {
    return employeeService.ingestData();
  }

  @GetMapping
  public List<Employee> getAllEmployees() throws IOException{
    return employeeService.getAllEmployees();
  }

  @GetMapping("/search/{searchString}")
  public List<Employee> getSpecificEmployees(@PathVariable String searchString) throws IOException{
    return employeeService.getSpecificEmployees(searchString);
  }
}
