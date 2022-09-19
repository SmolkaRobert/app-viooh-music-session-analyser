package app.services;

import app.models.UserTrackRecord;
import app.models.UserTrackTsvRecord;
import app.utils.UserTrackRecordMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class TsvUserTrackRecordsProcessor {

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder,
                              UserTrackTsvRecordTimestampExtractor userTrackTsvRecordTimestampExtractor,
                              @Value(value = "${app.kafka.topics.input.name}") String inputTopic,
                              @Value(value = "${app.kafka.topics.userTrackRecords.name}") String userTrackRecordsTopic,
                              Serde<UserTrackTsvRecord> userTrackTsvRecordSerde,
                              Serde<UserTrackRecord> userTrackRecordSerde,
                              Serde<String> stringSerde){
        streamsBuilder
                .stream(inputTopic,
                        Consumed
                                .with(stringSerde, userTrackTsvRecordSerde)
                                // force the stream to use the timestamps of the tracks as the events timestamps
                                .withTimestampExtractor(userTrackTsvRecordTimestampExtractor))
                .map((ignored, tsvRecord) -> KeyValue.pair(tsvRecord.userId(), UserTrackRecordMapper.from(tsvRecord)))
                .to(userTrackRecordsTopic,
                        Produced.with(stringSerde, userTrackRecordSerde));
    }
}
