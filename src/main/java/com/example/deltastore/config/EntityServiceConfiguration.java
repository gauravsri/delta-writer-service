package com.example.deltastore.config;

import com.example.deltastore.entity.GenericEntityService;
import com.example.deltastore.metrics.DeltaStoreMetrics;
import com.example.deltastore.schemas.User;
import com.example.deltastore.service.EntityService;
import com.example.deltastore.service.GenericEntityServiceImpl;
import com.example.deltastore.storage.DeltaTableManager;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Configuration for creating specialized EntityService beans for different entity types.
 * This allows us to use a single generic implementation while maintaining type safety
 * and dependency injection compatibility.
 */
@Configuration
@Profile("local")
public class EntityServiceConfiguration {

    @Bean
    @Qualifier("userEntityService")
    public EntityService<User> userEntityService(
            @Qualifier("optimized") DeltaTableManager deltaTableManager,
            DeltaStoreMetrics metrics,
            GenericEntityService genericEntityService) {
        return new GenericEntityServiceImpl<>("users", deltaTableManager, metrics, genericEntityService);
    }

    // Future entity services can be added here:
    /*
    @Bean
    @Qualifier("productEntityService")
    public EntityService<Product> productEntityService(
            @Qualifier("optimized") DeltaTableManager deltaTableManager,
            DeltaStoreMetrics metrics,
            GenericEntityService genericEntityService) {
        return new GenericEntityServiceImpl<>("products", deltaTableManager, metrics, genericEntityService);
    }
    */
}