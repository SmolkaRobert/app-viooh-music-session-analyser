package app.models;

public record UserTrackTsvRecord(String userId, long timestamp, String artistId, String artistName,
                                 String trackId, String trackName) {
}
