package app.serdes;

import app.models.UserTrackTsvRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.ZonedDateTime;
import java.util.Arrays;

import static java.lang.String.format;

@Component
public class UserTrackTsvRecordDeserialiser implements Deserializer<UserTrackTsvRecord> {

    private final String encoding;
    private final String columnSeparator;
    private final int numberOfColumnsInRecord;

    @Autowired
    public UserTrackTsvRecordDeserialiser(@Value(value = "${app.kafka.topics.input.encoding}") String encoding,
                                          @Value(value = "${app.kafka.topics.input.columnSeparator}") String columnSeparator,
                                          @Value(value = "${app.kafka.topics.input.numberOfColumnsInRecord}") int numberOfColumnsInRecord) {
        this.encoding = encoding;
        this.columnSeparator = columnSeparator;
        this.numberOfColumnsInRecord = numberOfColumnsInRecord;
    }

    @Override
    public UserTrackTsvRecord deserialize(String topic, byte[] data) {
        try {
            if (data == null) {
                return null;
            } else {
                String record = new String(data, encoding);
                return toUserTrackRecord(record);
            }
        } catch (Throwable throwable) {
            throw new SerializationException(format("Deserializing byte[]: %s failed!", data), throwable);
        }
    }

    private UserTrackTsvRecord toUserTrackRecord(String record) {
        String[] actualFields = record.split(columnSeparator, numberOfColumnsInRecord);
        String[] recordFields = prepareValidRecords(actualFields);

        String user = recordFields[0];

        long timestamp = ZonedDateTime.parse(recordFields[1]).toInstant().toEpochMilli();

        return new UserTrackTsvRecord(user, timestamp, recordFields[2], recordFields[3], recordFields[4], recordFields[5]);
    }

    /**
     * Adjusts any malformed records, which might be missing any of the columns by padding the end of the record
     * with empty Strings; this is far from perfect, since it might actually produce an invalid record, e.g.:
     * 1) the artistId could be placed in th timestamp column if the second is missing or,
     * 2) the trackName could be moved to the trackId, if the second was missing
     * Nonetheless, the tests on a significant portion of the LastFM data showed that this sort of validation is acceptable
     * for this task. Were it to be a real production application then more insightful and strict validation would have
     * to be implemented.
     */
    private String[] prepareValidRecords(String[] actualFields) {
        String[] recordFields = new String[numberOfColumnsInRecord];
        Arrays.fill(recordFields, "");

        System.arraycopy(actualFields, 0, recordFields, 0, actualFields.length);

        return recordFields;
    }
}
