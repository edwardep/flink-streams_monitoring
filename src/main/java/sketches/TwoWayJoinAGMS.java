package sketches;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.Math.sqrt;
import static sketches.SketchMath.*;

public class TwoWayJoinAGMS {

    private static class Bound {
        private int depth;
        private int width;
        private double T;                           // the threshold
        private List<Bilinear2d_safezone> zeta_2d;  // d-array of 2d bilinear safezones
        private SafezoneQuorum sz;                  // median quorum
        private double[] hat;

        /**
         * Initialize properly
         * @param _T
         * @param eikonal
         */
        public Bound(int d, int w, double _T, boolean eikonal) {
            this.depth = d;
            this.width = w;
            this.T = _T;

            zeta_2d = new ArrayList<>();
            sz = new SafezoneQuorum();
            sz.setEikonal(eikonal);
        }

        /**
         * Called during initialization
         * @param norm_xi
         * @param norm_psi
         */
        public void setup(double[] norm_xi, double[] norm_psi) {
            double[] zeta_E = new double[depth];

            for(int i = 0; i < depth; i++){
                // create the bilinear 2d safezones
                zeta_2d.add(new Bilinear2d_safezone(norm_xi[i], norm_psi[i], 4. * T));

                // compute the zeta_e vector for the median
                zeta_E[i] = zeta_2d.get(i).bilinear2d(norm_xi[i], norm_psi[i]) * sqrt(0.5);
            }
            // normalize the hat vector
            this.hat = normalize(hat, norm_xi);

            sz.prepare(zeta_E, (depth + 1) / 2);

        }

        /**
         * Computes the safezone of the median of the 2d safezone functions
         * @param x
         * @param y
         * @return
         */
        public double zeta(double[] x, double[] y) {

            double[] x2 = dotProduct(transform(x,depth,width), transform(hat,depth,width));
            double[] y2 = dotProduct(transform(y,depth,width), transform(y,depth,width));

            double[] zeta_X = new double[depth];
            for(int i = 0; i < depth; i++)
                zeta_X[i] = zeta_2d.get(i).bilinear2d(x2[i], sqrt(y2[i])) * sqrt(0.5);
            return sz.median(zeta_X);
        }
    }


    private int D;                     // sketch size
    private Bound lower, upper;        // the bounds objects

    public TwoWayJoinAGMS() {}

    /**
     * Construct a safe zone function object
     *
     * @param E is the reference point, which is the concatenation of two sketches
     * @param TLow  the lower bound
     * @param THigh the upper bound
     * @param eikonal   the eikonality flag
     */
    public TwoWayJoinAGMS(double[] E, int d, int w, double TLow, double THigh, boolean eikonal) {
        this.D = d*w;
        this.lower = new Bound(d, w, TLow, eikonal);
        this.upper = new Bound(d, w, -THigh, eikonal);

        assert E.length == 2*D;
        assert TLow < THigh;

        // Polarize the reference vector
        double[] s1 = Arrays.copyOfRange(E, 0, D);
        double[] s2 = Arrays.copyOfRange(E, D, 2*D);
        lower.hat = add(s1, s2);
        upper.hat = subtract(s1, s2);


        double[] norm_lower = normRow(transform(lower.hat, d, w));
        double[] norm_upper = normRow(transform(upper.hat, d, w));

        lower.setup(norm_lower, norm_upper);
        upper.setup(norm_upper, norm_lower);
    }

    public double inf(double[] X) {
        assert X.length == 2*D;

        // polarize
        double[] s1 = Arrays.copyOfRange(X, 0, D);
        double[] s2 = Arrays.copyOfRange(X, D, 2*D);

        double[] x = add(s1, s2);
        double[] y = subtract(s1, s2);

        return Math.min(lower.zeta(x, y), upper.zeta(y, x));
    }
}
