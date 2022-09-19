package app.services;

import app.models.UserTrackTsvRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.springframework.stereotype.Component;

@Component
public class UserTrackTsvRecordTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        return ((UserTrackTsvRecord) record.value()).timestamp();
    }
}
