package app.services;

import app.models.Track;
import app.models.UserSession;
import app.models.UserTrackRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;

@Component
public class UserTrackSessionGrouper {

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder,
                              @Value(value = "${app.kafka.topics.userTrackRecords.name}") String userTrackRecordsTopic,
                              @Value(value = "${app.kafka.topics.userSessions.name}") String userSessionsTopic,
                              Serde<UserTrackRecord> userTrackRecordSerde,
                              Serde<List<Track>> trackListSerde,
                              Serde<UserSession> userSessionSerde,
                              WindowedSerdes.SessionWindowedSerde<String> stringSessionWindowedSerde,
                              Serde<String> stringSerde,
                              @Value(value = "${app.musicSession.ttl}") Duration musicSessionTtl,
                              @Value(value = "${app.musicSession.gracePeriod}") Duration sessionGracePeriod){
        streamsBuilder
                .stream(userTrackRecordsTopic,
                        Consumed
                                .with(stringSerde, userTrackRecordSerde))
                .groupByKey()
                .windowedBy(SessionWindows.ofInactivityGapAndGrace(musicSessionTtl, sessionGracePeriod))
                /** My first instinct was store the tracks of each session in a Set. However, I switched to using a List
                 * instead, because first: there is a ready-to-use Serde for Lists and second: I wanted to account for
                 * the fact that a user might be logged-in on more than one device at the same time and could
                 * have more than one track record for the same song at exactly the same timestamp (unlikely but not
                 * impossible)
                 */
                .aggregate(this::prepareInitialiser,
                        (windowKey, record, sessionTracks) -> addTrack(sessionTracks, record.track()),
                        (windowKey, sameSessionTracks1, sameSessionTracks2) -> merge(sameSessionTracks1, sameSessionTracks2),
                        Materialized.with(stringSerde, trackListSerde)
                )
                .suppress(untilWindowCloses(unbounded()))
                .filter((windowedKey, sessionTracks) -> null != sessionTracks)
                .toStream()
                .map((windowedKey, sessionTracks) -> KeyValue.pair(windowedKey, new UserSession(windowedKey, sessionTracks)))
                .to(userSessionsTopic,
                        Produced.with(stringSessionWindowedSerde, userSessionSerde));
    }

    private List<Track> prepareInitialiser() {
        return new ArrayList<>();
    }

    private List<Track> merge(List<Track> list1, List<Track> list2) {
        List<Track> mergedLists = new ArrayList<Track>(list1);
        mergedLists.addAll(list2);
        return mergedLists;
    }

    private List<Track> addTrack(List<Track> list, Track element) {
        list.add(element);
        return list;
    }
}
