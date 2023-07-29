package com.learnkafkastreams.serdes;

import com.learnkafkastreams.domain.Order;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class JSONSerde {

    public static Serde<Order> orderFactory() {

        JSONSerializer<Order> jsonSerializer = new JSONSerializer<Order>();

        JSONDeserializer<Order> jsonDeSerializer = new JSONDeserializer<>(Order.class);
        return  Serdes.serdeFrom(jsonSerializer, jsonDeSerializer);
    }
}
