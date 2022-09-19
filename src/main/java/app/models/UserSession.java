package app.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.kafka.streams.kstream.Windowed;

import java.util.List;

public record UserSession(Windowed<String> userSessionId, List<Track> session) implements Comparable<UserSession> {

    @JsonIgnore
    public int getNumberOfTracks(){
        return session.size();
    }

    @JsonIgnore
    @Override
    public int compareTo(UserSession otherUserSession) {
        // compare in reverse order
        return Integer.compare(otherUserSession.getNumberOfTracks(), this.getNumberOfTracks());
    }
}
