package com.example.deltastore.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class OpenAPIConfig {

    @Bean
    public OpenAPI deltaStoreOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Delta Writer Service API")
                        .description("High-performance, scalable write-only Delta Lake service built with Spring Boot 3.2.5 and Delta Kernel 4.0.0. Features revolutionary zero-code entity management through a comprehensive generic architecture.")
                        .version("v3.0.0")
                        .contact(new Contact()
                                .name("Delta Writer Service Team")
                                .email("api-support@delta-writer.com")
                                .url("https://github.com/your-org/delta-writer-service"))
                        .license(new License()
                                .name("MIT License")
                                .url("https://opensource.org/licenses/MIT")))
                .servers(List.of(
                        new Server()
                                .url("http://localhost:8080")
                                .description("Development server"),
                        new Server()
                                .url("https://api.delta-writer.com")
                                .description("Production server")));
    }
}