package com.example.deltastore.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import com.example.deltastore.schemas.User;
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
        
        // Register Java Time Module for LocalDateTime support
        mapper.registerModule(new JavaTimeModule());
        
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
        
        // Add custom deserializer for User to handle JSON maps
        avroModule.addDeserializer(User.class, new JsonDeserializer<User>() {
            @Override
            public User deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
                JsonNode node = p.getCodec().readTree(p);
                return User.newBuilder()
                    .setUserId(node.get("user_id") != null ? node.get("user_id").asText() : null)
                    .setUsername(node.get("username") != null ? node.get("username").asText() : null)
                    .setEmail(node.get("email") != null && !node.get("email").isNull() ? 
                              node.get("email").asText() : null)
                    .setCountry(node.get("country") != null ? node.get("country").asText() : null)
                    .setSignupDate(node.get("signup_date") != null ? node.get("signup_date").asText() : null)
                    .build();
            }
        });
        
        mapper.registerModule(avroModule);
        return mapper;
    }
}