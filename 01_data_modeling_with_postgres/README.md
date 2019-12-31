# Sparkify
This project seeks to create a relational database for business analytics at Sparkify.  By creating an ETL pipeline we load historic songplay data for business anlaysis.  The company hopes to analyze artists and users to better match users with music they like and help us at Sparkify to advertise more relevant music to users.  We can also analyze what markets use our products the most to help us localize our advertising.  Not to mention, with this ETL we can discover new patterns within the data thanks to our lovely data scientists here at Sparkify.


### Usage
Run: 
```
python create_tables.py
python etl.py
```
Then, connect to the database with your favorite postgres client and make some queries.


### Project Files
```
data           - the directory of json files to process
etl.ipynb      - An explatory data notebook to understand the format of the data
test.ipynb     - Example notebook of how to connect, process, and query the resuling database from the ETL
etl.py         - The workhorse file of the ETL which processes the data files
README.py      - Overview information
sql_queries.py - Database queries to be string interpolated by data extrated from the json file in /data
```

## Project Summary

<img src="db-diagram.png" alt="Database diagram" width="650"/>

The data model is a typical star-shaped fact/dimension model.  The songplays represent measurable "metrics/business transactions" and the metadata surrounding the metrics is stored in various tables like 'song' and 'artist'.  This data model makes quering for analytics reqire few joins, thus increasing performance.  The 'level' attribute per user is replicated in the song plays table and the user table.  This can be used to do a more performant query on the number of paid plays by not using a JOIN of plays with users.

The data model includes several validity checks for longitude, latitude, and attribuetes related to time.  Updates to user data are resolved by a CONFLICT clause that simply accepts the updated data.  This works under the assumption that logs are processed chronologically and that the most recent data is accurate.  Additionally, redundant timestamps in the model are simply ignored on conflict.  Some of the artist data in the songplay logs has conflicting attributes and this is resolved by simply accepting updated attributes that change a null value to a valid value.  To acheive this the COALESCE clause is used to accept non-null values.

It seems that the data directory doesn't have many artists and songs that appear in the simulated log set.  Only 1 artist in the artist table was found in the songplay table.  However there was some interesting geographic data.  It seems most plays occured in the San Francisco Bay Area followed by Portland.  As a startup Sparkify may increase its revenue by targetting ads to people in the top markets or by focusing on adding new music to get users in weaker markets to use Sparkify more.


### Some example Queries 


__Top 20 Artists__


```python
%%sql SELECT name, COUNT(songplay.artist_id) as artistplays FROM songplay JOIN artist
ON songplay.artist_id=artist.artist_id GROUP BY name ORDER BY artistplays DESC LIMIT 20
```

     * postgresql://student:***@127.0.0.1/sparkifydb
    1 rows affected.





<table>
    <tr>
        <th>name</th>
        <th>artistplays</th>
    </tr>
    <tr>
        <td>Elena</td>
        <td>1</td>
    </tr>
</table>

__NOTE:__ There seems to be a lack of recognized artists and songs in the songplay logs

__How Many Non-None Song Plays__


```python
%sql SELECT * FROM songplay WHERE song_id != 'None';
```

     * postgresql://student:***@127.0.0.1/sparkifydb
    1 rows affected.





<table>
    <tr>
        <th>songplay_id</th>
        <th>start_time</th>
        <th>user_id</th>
        <th>level</th>
        <th>song_id</th>
        <th>artist_id</th>
        <th>session_id</th>
        <th>location</th>
        <th>user_agent</th>
    </tr>
    <tr>
        <td>6123</td>
        <td>2018-11-21 21:56:47.796000</td>
        <td>15</td>
        <td>paid</td>
        <td>SOZCTXZ12AB0182364</td>
        <td>AR5KOSW1187FB35FF4</td>
        <td>818</td>
        <td>Chicago-Naperville-Elgin, IL-IN-WI</td>
        <td>&quot;Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/36.0.1985.125 Chrome/36.0.1985.125 Safari/537.36&quot;</td>
    </tr>
</table>



__Top 20 Songs__


```python
%%sql SELECT title, name, COUNT(songplay.song_id) as totalplays FROM songplay JOIN song 
ON songplay.song_id=song.song_id JOIN artist ON song.artist_id=artist.artist_id
GROUP BY artist.name, song.title ORDER BY totalplays
LIMIT 20
```

     * postgresql://student:***@127.0.0.1/sparkifydb
    1 rows affected.





<table>
    <tr>
        <th>title</th>
        <th>name</th>
        <th>totalplays</th>
    </tr>
    <tr>
        <td>Setanta matins</td>
        <td>Elena</td>
        <td>1</td>
    </tr>
</table>



__Top 10 Geographic Locations__


```python
%%sql SELECT location, COUNT(location) as plays FROM songplay
GROUP BY location ORDER BY plays DESC LIMIT 10;
```

     * postgresql://student:***@127.0.0.1/sparkifydb
    10 rows affected.





<table>
    <tr>
        <th>location</th>
        <th>plays</th>
    </tr>
    <tr>
        <td>San Francisco-Oakland-Hayward, CA</td>
        <td>691</td>
    </tr>
    <tr>
        <td>Portland-South Portland, ME</td>
        <td>665</td>
    </tr>
    <tr>
        <td>Lansing-East Lansing, MI</td>
        <td>557</td>
    </tr>
    <tr>
        <td>Chicago-Naperville-Elgin, IL-IN-WI</td>
        <td>475</td>
    </tr>
    <tr>
        <td>Atlanta-Sandy Springs-Roswell, GA</td>
        <td>456</td>
    </tr>
    <tr>
        <td>Waterloo-Cedar Falls, IA</td>
        <td>397</td>
    </tr>
    <tr>
        <td>Lake Havasu City-Kingman, AZ</td>
        <td>321</td>
    </tr>
    <tr>
        <td>Tampa-St. Petersburg-Clearwater, FL</td>
        <td>307</td>
    </tr>
    <tr>
        <td>San Jose-Sunnyvale-Santa Clara, CA</td>
        <td>292</td>
    </tr>
    <tr>
        <td>Sacramento--Roseville--Arden-Arcade, CA</td>
        <td>270</td>
    </tr>
</table>



__Number of plays on Nov. 21, 2018__


```python
%%sql SELECT COUNT(songplay.start_time) FROM songplay JOIN time
ON songplay.start_time=time.start_time
WHERE year=2018 AND month=11 AND day=21 
```

     * postgresql://student:***@127.0.0.1/sparkifydb
    1 rows affected.





<table>
    <tr>
        <th>count</th>
    </tr>
    <tr>
        <td>437</td>
    </tr>
</table>




```python

```
