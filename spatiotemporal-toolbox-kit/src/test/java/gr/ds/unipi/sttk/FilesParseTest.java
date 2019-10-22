package gr.ds.unipi.sttk;

import org.davidmoten.hilbert.HilbertCurve;
import org.davidmoten.hilbert.SmallHilbertCurve;
import org.junit.Test;

import static org.junit.Assert.*;

public class FilesParseTest {

    @Test
    public void harvesine() {
        SmallHilbertCurve c = HilbertCurve.small().bits(1).dimensions(2);
//        c.ma
//        long index = c.index(2,1);
//        System.out.println(index);

        long[] point = c.point(3);
        System.out.println(point[0]+ "," +point[1]);


    }
}