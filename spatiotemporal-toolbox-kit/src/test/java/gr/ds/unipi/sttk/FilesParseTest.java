package gr.ds.unipi.sttk;

import com.github.davidmoten.guavamini.Preconditions;
import org.davidmoten.hilbert.HilbertCurve;
import org.davidmoten.hilbert.Range;
import org.davidmoten.hilbert.SmallHilbertCurve;
import org.junit.Test;

import java.util.List;

public class FilesParseTest {

    @Test
    public void harvesine() {
        int bits = 2;
        long maxOrdinates = 1L << bits;


        SmallHilbertCurve c = HilbertCurve.small().bits(bits).dimensions(3);

        //System.out.println(maxOrdinates);
        //long index = c.index(scalePoint(4,5,5,maxOrdinates));
        //System.out.println(index);

        List<Range> rangesList = c.query(new long[]{3,3,5},new long[]{8,10,6},Integer.MAX_VALUE);

        rangesList.stream().forEach(i->{
            System.out.println(i);
        });


//        long[] point = c.point(3);
//        System.out.println(point[0]+ "," +point[1]);


    }

    public static long[] scalePoint(long x1, long y1, long z1, long max) {
        long x = scale(((float) x1 - 1) / (5 - 1), max);
        long y = scale(((float) y1 - 1) / (5 - 1), max);
        long z = scale(((float) z1 - 1) / (5 - 1), max);
        return new long[] { x, y, z };
    }

    private static long scale(float d, long max) {
        Preconditions.checkArgument(d >= 0 && d <= 1);
        if (d == 1) {
            return max;
        } else {
            return Math.round(Math.floor(d * (max + 1)));
        }
    }
}