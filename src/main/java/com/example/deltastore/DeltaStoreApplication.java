package com.example.deltastore;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DeltaStoreApplication {

    public static void main(String[] args) {
        SpringApplication.run(DeltaStoreApplication.class, args);
    }

}
