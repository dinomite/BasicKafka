package com.clearspring.basic.kafka;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.javaapi.producer.SyncProducer;
import kafka.message.Message;
import kafka.producer.SyncProducerConfig;

public class HelloWorld {
    public static void main( String[] args ) throws IOException {
        Properties props = new Properties();
        props.put("host", "apb07");
        props.put("port", "9092");
        SyncProducer producer = new SyncProducer(new SyncProducerConfig(props));
        String topic = "test-topic";

        String helloWorld = "Hello, World!";
        List<Message> messages = new LinkedList<Message>();
        messages.add(new Message(helloWorld.getBytes()));
        producer.send(topic, new ByteBufferMessageSet(messages));
        System.out.println("Sent message to \"" + topic + "\" topic");

        producer.close();
    }
}
