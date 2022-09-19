package app.serdes;

import app.models.UserTrackTsvRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.StringJoiner;

import static java.lang.String.format;

@Component
public class UserTrackTsvRecordSerialiser implements Serializer<UserTrackTsvRecord> {

    private final String encoding;
    private final String columnSeparator;

    @Autowired
    public UserTrackTsvRecordSerialiser(@Value(value = "${app.kafka.topics.input.encoding}") String encoding,
                                        @Value(value = "${app.kafka.topics.input.columnSeparator}") String columnSeparator) {
        this.encoding = encoding;
        this.columnSeparator = columnSeparator;
    }

    @Override
    public byte[] serialize(String topic, UserTrackTsvRecord data) {
        try {
            if (data == null) {
                return null;
            } else {
                String record = mapToString(data);
                return record.getBytes(encoding);
            }
        } catch (Throwable throwable) {
            throw new SerializationException(format("Serializing: %s to byte[] failed due to following error:  ", data),
                    throwable);
        }
    }

    private String mapToString(UserTrackTsvRecord dto) {
        StringJoiner stringJoiner = new StringJoiner(columnSeparator);

        String zonedDateTimeString = Optional.ofNullable(dto.timestamp())
                .map(Instant::ofEpochMilli)
                .map(instant -> ZonedDateTime.ofInstant(instant, ZoneOffset.UTC))
                .map(ZonedDateTime::toString)
                .orElse("");

        return stringJoiner
                .add(getStringOrEmpty(dto.userId()))
                .add(zonedDateTimeString)
                .add(getStringOrEmpty(dto.artistId()))
                .add(getStringOrEmpty(dto.artistName()))
                .add(getStringOrEmpty(dto.trackId()))
                .add(getStringOrEmpty(dto.trackName()))
                .toString();
    }

    private static String getStringOrEmpty(Object object) {
        return Optional.ofNullable(object.toString()).orElse("");
    }
}
