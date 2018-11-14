import re
from django.db import connection
import json

def get_data_set_data(data_set_ids, types=[], bounds=[], min_depth=0, max_depth=0):
    """
    Get data for a multiple datasets as a list of hierachical objects.

    Keyword args:
    data_set_ids -- The database id of the datasetself.
    types       -- Commaseparated string, data types from the data_types table.

    Example: get_data_set_data(724, "CTDTMP,SALNTY")

    Returns:
    {
        data_columns:
        [
            "depth_id",
            "depth",
            "latitude",
            "longitude",
            "date_and_time",
            "temperature_value",
            "salinity_value",
        ],
        data_sets:
        [
            {
                dataset_id: 724,
                expocode: "74DI20110715",
                stations:
                [
                    {
                        station_id: 1234,
                        station_number: 12,
                        latitude: 60.13,
                        longitude: 10.10,
                        casts: [
                            {
                                "cast_id": 5678,
                                "cast_no": 1,
                                "depth_id": [
                                1234,
                                1235,
                                ...
                                ],
                                "depth": [
                                10.1,
                                20.5,
                                ...
                                ],
                                "date_and_time": [
                                "1985-01-01 20:00:00",
                                "1985-01-01 20:00:00",
                                ...
                                ],
                                "temperature": [
                                1.1432,
                                1.1333,
                                ...
                                ],
                                "salinity": [
                                35.4504,
                                35.4550,
                                ...
                                ],
                            },
                        ],
                    },
                ],
            },
            {
                data_set_id: 725,
                ...
            },
            ...
        ]
    }
    """

    cursor = connection.cursor()
    result = []
    select = """
        SELECT original_label, id from d2qc_data_types
    """
    cursor.execute(select)
    typelist = dict([(type[0], type[1]) for type in cursor.fetchall()])
    if not types:
            types.extend(['temperature'])

    select = """
        SELECT ds.id as data_set_id, ds.expocode,
        s.id as station_id, s.station_number,
        st_y(s.position) as latitude, st_x(s.position) as longitude,
        c.id as cast_id, c.cast as cast_no,
        d.id as depth_id, d.depth,
        d.date_and_time
    """
    frm = " FROM  d2qc_data_sets ds"
    join = " INNER JOIN d2qc_stations s ON (ds.id = s.data_set_id)"
    join += " INNER JOIN d2qc_casts c ON (s.id = c.station_id)"
    join += " INNER JOIN d2qc_depths d ON (c.id = d.cast_id)"
    ids = ','.join([str(i) for i in data_set_ids])
    where = " WHERE ds.id in (" + ids + ") "
    args = []
    not_all_nulls = []
    if len(types) > 0:
        for type in types:
            px = re.sub('[^a-zA-Z0-9]', '', type)
            select += ", {}.value AS {}_value".format(px, px)
            join += " LEFT OUTER JOIN d2qc_data_values {}".format(px)
            join += " ON (d.id = {}.depth_id".format(px)
            join += " AND {}.data_type_id = '{}')".format(px, typelist[type])
            not_all_nulls.append("{}.value".format(px))
        if len(bounds) == 4:
            where += """
            AND s.latitude between %s and %s
            AND s.longitude between %s and %s
            """
            args.extend(bounds)
        if min_depth > 0:
            where += 'AND d.depth > %s'
            args.append(min_depth)
        if max_depth > 0:
            where += 'AND d.depth < %s'
            args.append(max_depth)

    # Dont include if all values are null
    where += " AND ("
    where += ' IS NOT NULL OR '.join(not_all_nulls)
    where += ' IS NOT NULL'
    where += ")"


    order = " ORDER BY s.data_set_id ASC, s.id, c.id, d.id"
    cursor.execute(select + frm + join + where + order, args)

    data_set_id = ()
    station_id = ()
    cast_id = ()
    data = {}
    data_set = {}
    station = {}
    cast = {}
    output = {}
    output['data_columns'] = [col[0] for col in cursor.description][8:]
    output['data_sets'] = []
    for row in cursor.fetchall():
        if (row[0]) != data_set_id:
            if data_set_id:
                station['casts'].append(cast);
                data_set['stations'].append(station)
                output['data_sets'].append(data_set)
            data_set = {'stations':[]}
            station = {'casts':[]}
            station['station_id'] = row[2]
            station['station_number'] = row[3]
            station['latitude'] = row[4]
            station['longitude'] = row[5]
            cast = {}
            cast['cast_id'] = row[6]
            cast['cast_no'] = row[7]
            data_set['data_set_id'] = row[0]
            data_set['expocode'] = row[1]
            data_set_id = (row[0])
            station_id = (row[0], row[2])
            cast_id = (row[0], row[2], row[6])
        elif (row[0], row[2]) != station_id:
            if station_id:
                station['casts'].append(cast);
                data_set['stations'].append(station)
            cast = {}
            cast['cast_id'] = row[6]
            cast['cast_no'] = row[7]
            station = {'casts':[]}
            station['station_id'] = row[2]
            station['station_number'] = row[3]
            station['latitude'] = row[4]
            station['longitude'] = row[5]
            station_id = (row[0], row[2])
            cast_id = (row[0], row[2], row[6])
        elif (row[0], row[2], row[6]) != cast_id:
            if cast_id:
                station['casts'].append(cast);
            cast = {}
            cast['cast_id'] = row[6]
            cast['cast_no'] = row[7]
            cast_id = (row[0], row[2], row[6])
        for i, v in enumerate(row[8:]):
            if not output['data_columns'][i] in cast:
                cast[output['data_columns'][i]] = []
            cast[output['data_columns'][i]].append(v)

    if data_set_id:
        station['casts'].append(cast);
        data_set['stations'].append(station)
        output['data_sets'].append(data_set)

    return output

def dataset_extends(data_set_id, min_depth = 0):
    cursor = connection.cursor()
    select = """
            SELECT
            st_ymin(st_extent(position)), st_ymax(st_extent(position)),
            st_xmin(st_extent(position)), st_xmax(st_extent(position))
            FROM d2qc_stations s
    """
    args = []
    where = ' where data_set_id = %s '
    args.append(data_set_id)

    join = ''
    if min_depth > 0:
        join = " INNER JOIN d2qc_casts c on (c.station_id = s.id) "
        join += " INNER JOIN d2qc_depths d on (c.id = d.cast_id) "
        where += ' AND d.depth > %s'
        args.append(min_depth)
    print(select + join + where + " limit 5;")
    cursor.execute(select + join + where, args)
    return cursor.fetchone()

def get_xover_data_sets(data_set_id, range=200*1000, use_bbox=True):
    """Get all datasets that have stations within the range of stations in this
    dataset, using the provided range in meters.

    Keyword arguments:
    data_set_id -- The dataset for stations to query
    range       -- Default 200000m. Range in meters around stations that will
                   return a match
    use_bbox    -- Default True. Use the bounding box of the stations for the
                   dataset argument instead of the actual stations. This is
                   much faster but less accurate, and usually returns more
                   data sets than the more precise method.

    Example:
    >>> get_xover_data_sets(5, 100000)
    [2, 4, 6, 8, 11, 13, 15, 16, 19, 20, 226, 227, 239, 621, 701, 703]

    >>> get_xover_data_sets(5, 200000, False)
    [2, 4, 6, 8, 11, 13, 15, 16, 19, 20, 621, 701, 703]
    ... data_set_ids with stations within 100km of stations in data_set_id 5.


    """
    cursor = connection.cursor()
    select = """
            SELECT DISTINCT s.data_set_id
            FROM d2qc_stations s, d2qc_stations s2
            WHERE ST_DistanceSphere(s.position, s2.position) < %s
            AND s2.data_set_id=%s AND s.data_set_id<>%s
            ORDER BY s.data_set_id;
    """
    args = [range, data_set_id, data_set_id]
    if use_bbox:
        select = """
                SELECT DISTINCT s.data_set_id
                FROM d2qc_stations s, (
                    SELECT  ST_Extent(position) AS position
                    FROM d2qc_stations
                    WHERE data_set_id=%s
                ) s2
                WHERE ST_DistanceSphere(s.position, s2.position) < %s
                AND s.data_set_id<>%s
                ORDER BY s.data_set_id;
        """
        args = [data_set_id, range, data_set_id]


    cursor.execute(select, args)
    return [row[0] for row in cursor.fetchall()]

def get_data_set_center(data_set_id):
    """Get the center of a data set by taking the extent of the dataset
    and finding the middle.

    Returns [lon lat]
    """

    cursor = connection.cursor()
    select = """
            SELECT st_asgeojson(ST_Centroid(st_extent(position)))
            FROM d2qc_stations
            WHERE data_set_id = %s
    """
    args = [data_set_id]
    cursor.execute(select, args)
    return json.loads(cursor.fetchone()[0])['coordinates']
