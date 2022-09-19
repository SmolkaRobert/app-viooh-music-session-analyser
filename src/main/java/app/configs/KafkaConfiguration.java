package app.configs;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import app.models.*;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonSerde;
import app.serdes.UserTrackTsvRecordDeserialiser;
import app.serdes.UserTrackTsvRecordSerialiser;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaConfiguration {

    @Value(value = "${app.kafka.bootstrap-servers}")
    private String bootstrapAddress;

    @Value(value = "${app.kafka.streams.state.dir}")
    private String stateStoreLocation;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kafkaStreamsConfiguration() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(APPLICATION_ID_CONFIG, "app-viooh-music-session-analyser");
        properties.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        properties.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(STATE_DIR_CONFIG, stateStoreLocation);

        return new KafkaStreamsConfiguration(properties);
    }

    @Bean
    public Serde<UserTrackTsvRecord> userTrackTsvRecordSerde(UserTrackTsvRecordSerialiser userTrackTsvRecordSerialiser,
                                                             UserTrackTsvRecordDeserialiser userTrackTsvRecordDeserialiser) {
        return Serdes.serdeFrom(userTrackTsvRecordSerialiser, userTrackTsvRecordDeserialiser);
    }

    @Bean
    public Serde<UserTrackRecord> userTrackRecordSerde() {
        return new JsonSerde<>(UserTrackRecord.class);
    }

    @Bean
    public Serde<Track> trackSerde() {
        return new JsonSerde<>(Track.class);
    }

    @Bean
    public Serde<UserSession> userSessionSerde() {
        return new JsonSerde<>(UserSession.class);
    }

    @Bean
    public Serde<TopSessions> topSessionsSerde() {
        return new JsonSerde<>(TopSessions.class);
    }

    @Bean
    public Serde<List<Track>> trackListSerde(Serde<Track> trackSerde) {
        return Serdes.ListSerde(ArrayList.class, trackSerde);
    }

    @Bean
    public WindowedSerdes.SessionWindowedSerde<String> stringSessionWindowedSerde(Serde<String> stringSerde) {
        return new WindowedSerdes.SessionWindowedSerde<>(stringSerde);
    }

    @Bean
    public Serde<String> stringSerde() {
        return Serdes.String();
    }
}