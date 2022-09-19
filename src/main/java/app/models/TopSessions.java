package app.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.TreeSet;
import java.util.stream.Collectors;

public class TopSessions {

    private static final double MINIMUM_CAPACITY_OVERLOAD = 1.0;
    private static final double MAXIMUM_CAPACITY_OVERLOAD = 1.2;

    @JsonProperty
    private final int capacity;
    @JsonProperty
    private final double capacityOverloadRate;

    @JsonProperty
    private TreeSet<UserSession> sessions;

    public TopSessions(int capacity, double capacityOverloadRate) {
        this(capacity, capacityOverloadRate, new TreeSet<>());
    }

    @JsonCreator
    public TopSessions(@JsonProperty("capacity") int capacity,
                       @JsonProperty("capacityOverloadRate") double capacityOverloadRate,
                       @JsonProperty("sessions") TreeSet<UserSession> sessions) {
        this.capacity = capacity;
        this.capacityOverloadRate = optionallyAdjustCapacityOverloadRate(capacityOverloadRate);
        this.sessions = sessions;
    }

    private static double optionallyAdjustCapacityOverloadRate(double capacityOverloadRate) {
        if (capacityOverloadRate < MINIMUM_CAPACITY_OVERLOAD) {
            return MINIMUM_CAPACITY_OVERLOAD;
        } else {
            return Math.min(capacityOverloadRate, MAXIMUM_CAPACITY_OVERLOAD);
        }
    }

    @JsonIgnore
    public TreeSet<UserSession> getTop() {
        limitTopSessions();
        return sessions;
    }

    @JsonIgnore
    public TopSessions add(UserSession userSession) {
        sessions.add(userSession);
        optionallyLimitTopSessionToCapacity();
        return this;
    }

    private void optionallyLimitTopSessionToCapacity() {
        if (isCapacityOverloadSurpassed()) {
            limitTopSessions();
        }
    }

    private void limitTopSessions() {
        sessions =
                sessions.stream()
                        .limit(capacity)
                        .collect(Collectors.toCollection(TreeSet::new));
    }

    private boolean isCapacityOverloadSurpassed() {
        return sessions.size() > (int) (capacity * capacityOverloadRate);
    }

}
