package de.leipzig.htwk.gitrdf.sparql.query.timemeasurement;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
@NoArgsConstructor
public class TimeLog {
    private long entryId;
    private long totalTime;

    public void printTimes() {
        log.info("Identifier for measurements is: '{}'", this.entryId);
        log.info("TIME MEASUREMENT DONE: Total time in milliseconds is: '{}'", this.totalTime);

    }
}
