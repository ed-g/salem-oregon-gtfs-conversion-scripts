/* 

    Copyright (c) 2016 Trillium Transit 

    Permission is hereby granted, free of charge, to any person obtaining a
    copy of this software and associated documentation files (the "Software"),
    to deal in the Software without restriction, including without limitation
    the rights to use, copy, modify, merge, publish, distribute, sublicense,
    and/or sell copies of the Software, and to permit persons to whom the
    Software is furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in
    all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
    FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
    DEALINGS IN THE SOFTWARE.  
    
    This is a PostgrSQL psql script file.

    To use, 

    (0) Save a backup copy of your original GTFS data, somewhere where this script
        does not have access to it!

    (1) Customize the :gtfs_dir to find your stop_times.txt file

    (2) Either:
        (a) Run from the shell:
            "psql  < modify-stop-times-remove-repeats.sql"
        (b) Start psql, open any database, and then run from psql.
            "\i modify-stop-times-remove-repeats.sql"

    (3) Verify stop_times_without_repeats.txt looks good, then copy it over
        stop_times.txt from the shell:
            "cp stop_times_without_repeats.txt stop_times.txt"


    (4) Create a new gtfs.zip file with updated stop_times.txt
        In Linux:
            "zip gtfs.zip *.txt"

    [...]

    (5) Profit.

 */


select current_database();

/*
    :gtfs_dir controls where we find gtfs files, such as stop_times.txt.

    Look for gtfs *.txt files in the current directory: 

        \set gtfs_dir `pwd`

    Or set to any directory where you extracted GTFS.zip file:

        \set gtfs_dir /path/to/your/gtfs/files/
*/
\set gtfs_dir `pwd`

\echo 'Looking for gtfs files in ' :gtfs_dir

/* 
    :temp_tables_option controls whether to create tempory tables (which are automatically
    dropped at the end of the psql session) or regular tables.

    To use temporary tables:

        \set temp_tables_option temp

    To use regular tables, for example to keep the stop_times table around for analysis:

        \set temp_tables_option ' '
 */
\set temp_tables_option temp

\echo 'temp_tables_option is ' :temp_tables_option

/* don't pause on long output */
\pset pager off

create :temp_tables_option table if not exists stops (
    stop_id text primary key,
    stop_code text,
    stop_name text,
    stop_desc text,
    stop_lat text,
    stop_lon text,
    zone_id text,
    stop_url text,
    location_type text,
    parent_station text,
    stop_timezone text,
    wheelchair_boarding text);


create :temp_tables_option table if not exists stop_times (
    trip_id text,
    arrival_time text,
    departure_time text,
    stop_id text,
    stop_sequence text,
    stop_headsign text,
    pickup_type text,
    drop_off_type text,
    shape_dist_traveled text,
    primary key (trip_id, stop_sequence)
    );

\set stops_file :gtfs_dir /stops.txt
\echo 'copying ' :'stops_file'
\set copycommand '\\copy "stops" from  ' :'stops_file' ' with (format csv, header true)'
:copycommand

\set stop_times_file :gtfs_dir /stop_times.txt
\echo 'copying ' :'stop_times_file'
\set copycommand '\\copy "stop_times" from  ' :'stop_times_file' ' with (format csv, header true)'
:copycommand

create :temp_tables_option view find_repeated_stops AS 
(
    select 
        trip_id, stop_sequence,
        (stop_id = lead(stop_id) over w) as first_of_repeat,
        (stop_id = lag(stop_id)  over w) as second_of_repeat,
        arrival_time,
        departure_time,
        lead(departure_time) over w as departure_time_next
    from stop_times 
    window w as (partition by trip_id order by stop_sequence) 
    order by trip_id, stop_sequence
);

\echo 'Repeated stops:'
select * from find_repeated_stops r 
where first_of_repeat or second_of_repeat;


UPDATE stop_times s
    SET 
        departure_time = r.departure_time_next,
        pickup_type = '0'
    FROM find_repeated_stops r
    WHERE 
        (s.trip_id = r.trip_id AND s.stop_sequence = r.stop_sequence)
        AND first_of_repeat;

DELETE 
    FROM stop_times s
    USING find_repeated_stops r
    WHERE 
        (s.trip_id = r.trip_id AND s.stop_sequence = r.stop_sequence)
        AND second_of_repeat;


\echo 'Repeated stops:'
select * 
from find_repeated_stops r 
where first_of_repeat or second_of_repeat;


\set stop_times_without_repeats :gtfs_dir /stop_times_without_repeats.txt
\echo 'copying ' :'stop_times_without_repeats'
\set copycommand '\\copy "stop_times" to  ' :'stop_times_without_repeats' ' with (format csv, header true)'
:copycommand


