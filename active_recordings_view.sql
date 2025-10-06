-- Active Recordings View
-- Translated from JFR view.ini format to DuckDB SQL

CREATE OR REPLACE VIEW environment_active_recordings AS
SELECT
    LAST(recordingStart, ActiveRecording) AS "Start",
    LAST(recordingDuration, ActiveRecording) AS "Duration",
    LAST(name, ActiveRecording) AS "Name",
    LAST(destination, ActiveRecording) AS "Destination",
    LAST(maxAge, ActiveRecording) AS "Max Age",
    LAST(maxSize, ActiveRecording) AS "Max Size"
FROM ActiveRecording
GROUP BY id
ORDER BY "Start";

-- Example usage:
-- SELECT * FROM environment_active_recordings;