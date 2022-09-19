package app.services;

import app.models.Track;
import app.models.UserSession;
import app.models.UserTrackRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class ArtificialSessionsKeyRewriter {

    private static final String ARTIFICIAL_CONSTANT_KEY = "artificial-constant-key";

    /** Due to the lack of Kafka related knowledge I had no idea how to process the sessions in KStreams/KTable in
     * an effective and stateful manner; therefore I proposed to force them all to have the same key in order to
     * group them all next in the {@link TopSessionRetriever#buildPipeline(StreamsBuilder, String, String, Serde, Serde, Serde, WindowedSerdes.SessionWindowedSerde, Serde, int, double)}
     */
    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder,
                              @Value(value = "${app.kafka.topics.userSessions.name}") String userSessionsTopic,
                              @Value(value = "${app.kafka.topics.allSessionUnderCommonKey.name}") String allSessionUnderCommonKeyTopic,
                              Serde<UserSession> userSessionSerde,
                              Serde<String> stringSerde,
                              WindowedSerdes.SessionWindowedSerde<String> stringSessionWindowedSerde) {
        streamsBuilder
                .stream(userSessionsTopic,
                        Consumed.with(stringSessionWindowedSerde, userSessionSerde))
                .map((sessionKey, userSession) -> KeyValue.pair(ARTIFICIAL_CONSTANT_KEY, userSession))
                .to(allSessionUnderCommonKeyTopic,
                        Produced.with(stringSerde, userSessionSerde));
    }
}
