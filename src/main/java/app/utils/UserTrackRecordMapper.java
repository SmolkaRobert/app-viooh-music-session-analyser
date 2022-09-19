package app.utils;

import app.models.Track;
import app.models.UserTrackRecord;
import app.models.UserTrackTsvRecord;

public class UserTrackRecordMapper {

    public static UserTrackRecord from(UserTrackTsvRecord tsvRecord) {
        Track track = new Track(tsvRecord.timestamp(), tsvRecord.artistId(), tsvRecord.artistName(), tsvRecord.trackId(),
                                tsvRecord.trackName());
        return new UserTrackRecord(tsvRecord.userId(), track);
    }
}
