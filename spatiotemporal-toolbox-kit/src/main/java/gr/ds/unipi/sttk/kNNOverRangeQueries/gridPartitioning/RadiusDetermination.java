package gr.ds.unipi.sttk.kNNOverRangeQueries.gridPartitioning;

import gr.ds.unipi.sttk.FilesParse;

import java.util.Map;

public class RadiusDetermination {

    private final Map<Long, Long> histogram;
    private final double x;
    private final double y;
    private final long minXc;
    private final long maxXc;
    private final long minYc;
    private final long maxYc;
    private final double minx;
    private final double miny;
    private long numberOfCellsxAxis;
    private long numberOfCellsyAxis;


    private RadiusDetermination(Map<Long, Long> histogram, long numberOfCellsxAxis, long numberOfCellsyAxis, double minx, double miny, double maxx, double maxy) {
        this.numberOfCellsxAxis = numberOfCellsxAxis;
        this.numberOfCellsyAxis = numberOfCellsyAxis;
        this.histogram = histogram;
        x = (maxx - minx) / numberOfCellsxAxis;
        y = (maxy - miny) / numberOfCellsyAxis;

        minXc = 0l;
        maxXc = numberOfCellsxAxis - 1l;

        minYc = 0l;
        maxYc = numberOfCellsyAxis - 1l;

//        minXc = (long) (minx / x);
//        maxXc = minXc + numberOfCellsxAxis - 1;
//
//        minYc = (long) (miny / y);
//        maxYc = minYc + numberOfCellsyAxis - 1;

        this.minx = minx;
        this.miny = miny;
    }

    public static RadiusDetermination newRadiusDetermination(Map<Long, Long> histogram, long numberOfCellsxAxis, long numberOfCellsyAxis, double minx, double miny, double maxx, double maxy) {

        return new RadiusDetermination(histogram, numberOfCellsxAxis, numberOfCellsyAxis, minx, miny, maxx, maxy);
    }

    private long getIdCellOfPoint(double x, double y) {

//        long xc = (long) (x / this.x);
//        long yc = (long) (y / this.y);

        long xc = (long) ((x - minx) / this.x);
        long yc = (long) ((y - miny) / this.y);

        return (xc + (yc * numberOfCellsxAxis));
    }

    private double findTheMaxCornerDistance(double x, double y, long xc, long yc/*, long id*/) {

//        long xc = (id % numberOfCellsxAxis);
//        long yc = (id / numberOfCellsxAxis);

//        double upperBoundx = ((xc + 1) * this.x);
//        double upperBoundy = ((yc + 1) * this.y);
//
//        double lowerBoundx = (xc * this.x);
//        double lowerBoundy = (yc * this.y);

        double upperBoundx = ((xc + 1) * this.x) + minx;
        double upperBoundy = ((yc + 1) * this.y) + miny;

        double lowerBoundx = (xc * this.x) + minx;
        double lowerBoundy = (yc * this.y) + miny;

        double distance;

        double d1 = FilesParse.harvesine(x, y, upperBoundx, upperBoundy);
        //System.out.println("coordinates " + upperBoundx +" - "+ upperBoundy);
        distance = d1;

        double d2 = FilesParse.harvesine(x, y, lowerBoundx, lowerBoundy);
        //System.out.println("coordinates " + lowerBoundx +" - "+ lowerBoundy);

        if (Double.compare(d2, distance) == 1) {
            distance = d2;
        }

        double d3 = FilesParse.harvesine(x, y, upperBoundx, lowerBoundy);
        //System.out.println("coordinates " + upperBoundx +" - "+ lowerBoundy);

        if (Double.compare(d3, distance) == 1) {
            distance = d3;
        }

        double d4 = FilesParse.harvesine(x, y, lowerBoundx, upperBoundy);
        //System.out.println("coordinates " + lowerBoundx +" - "+ upperBoundy);

        if (Double.compare(d4, distance) == 1) {
            distance = d4;
        }

        return distance;

    }

    private long getNumberOfCell(long cellId) {

        if (histogram.containsKey(cellId)) {
            return histogram.get(cellId);
        } else {
            return 0;
        }
    }

    public double findRadius(double x, double y, long neighboors) {

//        long xc = (long) (x / this.x);
//        long yc = (long) (y / this.y);

        long xc = (long) ((x - minx) / this.x);
        long yc = (long) ((y - miny) / this.y);

        double distance = Integer.MIN_VALUE;

        long k = 0;
        long points = 0;

        points = getNumberOfCell(xc + (yc * numberOfCellsxAxis));

        if (points >= neighboors) {
            distance = findTheMaxCornerDistance(x, y, xc, yc);
            //distance = findTheMaxCornerDistance(x, y, (xc + (yc * numberOfCellsxAxis)));
        } else {
            k++;
        }

        while (k > 0) {

            if ((xc - k) >= minXc) {
                for (long i = yc - k; i <= yc + k; i++) {
                    if (i < minYc || i > maxYc) {
                        continue;
                    }
                    points = points + getNumberOfCell((xc - k) + (i * numberOfCellsxAxis));
                }
            }


            if ((xc + k) <= maxXc) {
                for (long i = yc - k; i <= yc + k; i++) {
                    if (i < minYc || i > maxYc) {
                        continue;
                    }
                    points = points + getNumberOfCell((xc + k) + (i * numberOfCellsxAxis));
                }
            }

            if ((yc - k) >= minYc) {
                for (long i = xc - k + 1; i < xc + k; i++) {
                    if (i < minXc || i > maxXc) {
                        continue;
                    }
                    points = points + getNumberOfCell(i + ((yc - k) * numberOfCellsxAxis));
                }
            }


            if ((yc + k) <= maxYc) {
                for (long i = xc - k + 1; i < xc + k; i++) {
                    if (i < minXc || i > maxXc) {
                        continue;
                    }
                    points = points + getNumberOfCell(i + ((yc + k) * numberOfCellsxAxis));
                }
            }

            if (points < neighboors) {
                k++;

            } else {


                long MaximumXc = xc + k;
                long MinimumXc = xc - k;

                long MaximumYc = yc + k;
                long MinimumYc = yc - k;

                if (xc + k > maxXc) {
                    MaximumXc = maxXc;
                }
                if (xc - k < minXc) {
                    MinimumXc = minXc;
                }
                if (yc + k > maxYc) {
                    MaximumYc = maxYc;
                }
                if (yc - k < minYc) {
                    MinimumYc = minYc;
                }

                //double d1 = findTheMaxCornerDistance(x, y, (MinimumXc + (MinimumYc * numberOfCellsxAxis)));
                double d1 = findTheMaxCornerDistance(x, y, MinimumXc, MinimumYc);

                if (distance < d1) {
                    distance = d1;
                }

                //double d2 = findTheMaxCornerDistance(x, y, (MaximumXc + (MaximumYc * numberOfCellsxAxis)));
                double d2 = findTheMaxCornerDistance(x, y, MaximumXc, MaximumYc);

                if (distance < d2) {
                    distance = d2;
                }

                //double d3 = findTheMaxCornerDistance(x, y, (MaximumXc + (MinimumYc * numberOfCellsxAxis)));
                double d3 = findTheMaxCornerDistance(x, y, MaximumXc, MinimumYc);

                if (distance < d3) {
                    distance = d3;
                }

                //double d4 = findTheMaxCornerDistance(x, y, (MinimumXc + (MaximumYc * numberOfCellsxAxis)));
                double d4 = findTheMaxCornerDistance(x, y, MinimumXc, MaximumYc);

                if (distance < d4) {
                    distance = d4;
                }

                System.out.println("k= " + k);

                k = -1;
            }
        }
        return distance;
    }
}
