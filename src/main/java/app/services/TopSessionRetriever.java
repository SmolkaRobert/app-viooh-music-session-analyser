package app.services;

import app.models.TopSessions;
import app.models.Track;
import app.models.UserSession;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class TopSessionRetriever {

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder,
                              @Value(value = "${app.kafka.topics.allSessionUnderCommonKey.name}") String allSessionUnderCommonKeyTopic,
                              @Value(value = "${app.kafka.topics.topSessions.name}") String topSessionsTopic,
                              Serde<UserSession> userSessionSerde,
                              Serde<String> stringSerde,
                              Serde<TopSessions> topSessionsSerde,
                              WindowedSerdes.SessionWindowedSerde<String> stringSessionWindowedSerde,
                              Serde<List<Track>> trackListSerde,
                              @Value(value = "${app.topSessions.capacity.value}") int topSessionsCapacity,
                              @Value(value = "${app.topSessions.capacity.overloadRate}") double topSessionsCapacityOverloadRate) {
        streamsBuilder
                .stream(allSessionUnderCommonKeyTopic,
                        Consumed.with(stringSerde, userSessionSerde))
                .groupByKey()
                /** The idea was to get the sessions from the {@link ArtificialSessionsKeyRewriter} grouped together, since they
                 * would now share the same key and pass the to the {@link TopSessions} object
                 */
                .aggregate(() -> new TopSessions(topSessionsCapacity, topSessionsCapacityOverloadRate),
                        /** There is some minor clean-up performed inside {@link TopSessions#add(UserSession)} in order
                         * to restrain the Set inside from becoming overly large; the clean-up, however, in order not
                         * to slow the entire processing too much
                         */
                        (ignored, userSession, topSessions) -> topSessions.add(userSession),
                        Materialized.with(stringSerde, topSessionsSerde))
                .toStream()
                .flatMap((ignored, topSessions) ->
                        /** Now only top N (identified with the topSessionsCapacity parameter) sessions would be
                         * published once to the output topic; however, a proper windowing and suppression has to be
                         * introduced; otherwise all the updates are triggering new records being published
                         */
                        topSessions.getTop().stream()
                                .map(userSession -> KeyValue.pair(userSession.userSessionId(), userSession.session()))
                                .collect(Collectors.toCollection(LinkedList::new)))
                .to(topSessionsTopic,
                        Produced.with(stringSessionWindowedSerde, trackListSerde));
    }
}
