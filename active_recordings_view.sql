-- Active Recordings View
-- Translated from JFR view.ini format to DuckDB SQL

CREATE OR REPLACE VIEW environment_active_recordings AS
SELECT
    LAST(recordingStart) AS "Start",
    LAST(recordingDuration) AS "Duration",
    LAST(name) AS "Name",
    LAST(destination) AS "Destination",
    LAST(maxAge) AS "Max Age",
    LAST(maxSize) AS "Max Size"
FROM ActiveRecording
GROUP BY id
ORDER BY "Start";

-- Example usage:
-- SELECT * FROM environment_active_recordings;