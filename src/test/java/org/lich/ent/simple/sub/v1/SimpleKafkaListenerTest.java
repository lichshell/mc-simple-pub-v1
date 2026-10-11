package org.lich.ent.simple.sub.v1;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class SimpleKafkaListenerTest {

    @Test
    void onMessage() {
        // Arrange: create a sample incoming Kafka record
        ConsumerRecord<String, String> record =
                new ConsumerRecord<>(
                        "simple.msg.topic.v1",
                        0,
                        25L,
                        "employee-101",
                        "{\"employeeId\":101,\"name\":\"John\"}"
                );

        Acknowledgment acknowledgment =
                mock(Acknowledgment.class);

        SimpleKafkaListener listener = new SimpleKafkaListener();

        // Act: invoke the listener
        listener.onMessageV1(record, acknowledgment);

        // Assert: verify acknowledgement was requested
        verify(acknowledgment).acknowledge();
    }
}