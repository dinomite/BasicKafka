package com.clearspring.basic.kafka;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.javaapi.producer.SyncProducer;
import kafka.message.Message;
import kafka.producer.SyncProducerConfig;

public class CommandLineProducer {
    public static void main( String[] args ) throws IOException {
        Properties props = new Properties();
        props.put("host", "apb07");
        props.put("port", "9092");
        SyncProducer producer = new SyncProducer(new SyncProducerConfig(props));
        String topic = "test-topic";

        BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
        boolean done = false;
        while (!done) {
            String line = input.readLine();
            if(line == null) {
                done = true;
            } else {
                List<Message> messages = new LinkedList<Message>();
                messages.add(new Message(line.getBytes()));
                producer.send(topic, 0, new ByteBufferMessageSet(messages));
                System.out.println("Sent " + messages.size() + " messges");
            }
        }

        producer.close();
    }
}
