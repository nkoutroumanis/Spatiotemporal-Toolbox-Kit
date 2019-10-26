package gr.ds.unipi.sttk.dbDataInsertion;

import com.github.davidmoten.guavamini.Preconditions;

public class GeoUtil {

    public static long[] scalePoint(double lon, double minLon, double maxLon, double lat, double minLat, double maxLat, long time, long minTime, long maxTime,
                                    long max) {
        long x = scale(( lon - minLon) / (maxLon - minLon), max);
        long y = scale(( lat - minLat) / (maxLat - minLat), max);
        long z = scale(((double) time - minTime) / (maxTime - minTime), max);
        return new long[] { x, y, z };
    }

    private static long scale(double d, long max) {
        Preconditions.checkArgument(d >= 0 && d <= 1);
        if (d == 1) {
            return max;
        } else {
            return Math.round(Math.floor(d * (max + 1)));
        }
    }

}

