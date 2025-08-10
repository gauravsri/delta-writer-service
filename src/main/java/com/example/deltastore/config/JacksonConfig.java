package com.example.deltastore.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.io.IOException;

@Configuration
public class JacksonConfig {

    @Bean
    @Primary
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        
        // Disable writing dates as timestamps
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        
        // Use snake_case property naming to match Avro field names
        mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        
        // Create a custom module for Avro serialization
        SimpleModule avroModule = new SimpleModule();
        
        // Add custom serializer for Schema to prevent circular reference
        avroModule.addSerializer(Schema.class, new JsonSerializer<Schema>() {
            @Override
            public void serialize(Schema schema, JsonGenerator gen, SerializerProvider serializers) throws IOException {
                gen.writeString(schema.toString());
            }
        });
        
        // Add custom serializer for GenericRecord
        avroModule.addSerializer(GenericRecord.class, new JsonSerializer<GenericRecord>() {
            @Override
            public void serialize(GenericRecord record, JsonGenerator gen, SerializerProvider serializers) throws IOException {
                gen.writeStartObject();
                for (Schema.Field field : record.getSchema().getFields()) {
                    Object value = record.get(field.name());
                    if (value != null) {
                        gen.writeObjectField(field.name(), value);
                    } else {
                        gen.writeNullField(field.name());
                    }
                }
                gen.writeEndObject();
            }
        });
        
        mapper.registerModule(avroModule);
        return mapper;
    }
}