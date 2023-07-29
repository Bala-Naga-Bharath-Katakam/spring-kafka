package com.learnkafkastreams.serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;

public class JSONDeserializer<T> implements Deserializer<T> {

    private Class<T> mappingClass;

    public JSONDeserializer(Class<T> mappingClass) {
        this.mappingClass = mappingClass;
    }

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(new String(data, StandardCharsets.UTF_8), mappingClass);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
