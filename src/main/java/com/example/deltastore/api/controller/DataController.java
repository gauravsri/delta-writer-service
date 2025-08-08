package com.example.deltastore.api.controller;

import com.example.deltastore.service.DataService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/data")
@RequiredArgsConstructor
public class DataController {

    private final DataService dataService;

    @PostMapping("/{tableName}")
    public ResponseEntity<Void> createData(
            @PathVariable String tableName,
            @RequestBody Map<String, Object> data) {

        dataService.saveData(tableName, data);

        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    @GetMapping("/{tableName}/{primaryKey}")
    public ResponseEntity<Map<String, Object>> readData(
            @PathVariable String tableName,
            @PathVariable String primaryKey) {

        return dataService.readData(tableName, primaryKey)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/{tableName}")
    public ResponseEntity<List<Map<String, Object>>> readTable(
            @PathVariable String tableName,
            @RequestParam Map<String, String> partitionFilters) {

        return ResponseEntity.ok(dataService.readByPartitions(tableName, partitionFilters));
    }
}
