-- real-time sql analysis on streaming input with a window of 1000 records
CREATE OR REPLACE STREAM "DESTINATION_SQL_STREAM" (
    "incident_longitude" DOUBLE,
    "incident_latitude" DOUBLE,
    HOTSPOTS_RESULT VARCHAR(10000)
);
CREATE OR REPLACE PUMP "STREAM_PUMP" AS INSERT INTO "DESTINATION_SQL_STREAM"
    SELECT "longitude", "latitude", "HOTSPOTS_RESULT" FROM
        TABLE(HOTSPOTS(
            CURSOR(SELECT STREAM * FROM "SOURCE_SQL_STREAM_001"),
            1000,
            0.013,
            10
        )
    );
