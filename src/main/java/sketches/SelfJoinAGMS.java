package sketches;

import fgm.SafeZone;

import java.util.Arrays;

import static java.lang.Math.min;
import static sketches.SketchMath.*;

public class SelfJoinAGMS extends SafeZone {

    static class SelfJoin_upperBound {
        private double[] sqrt_T;
        private SafezoneQuorum sz;

        /**
         * The safe zone function for the upper bound on the self-join estimate of an AGMS sketch.
         * The overall safe zone is defined as the median quorum over these values.
         * @param E The reference vector
         * @param T The threshold value
         * @param eikonal The eikonality flag
         */
        SelfJoin_upperBound(double[][] E, double T, boolean eikonal) {
            this.sqrt_T = new double[E.length];
            Arrays.fill(this.sqrt_T, Math.sqrt(T));

            double[] dest = subtract(sqrt_T, normRow(E)); //todo: reverse order
            sz = new SafezoneQuorum(dest, (E.length+1)/2, eikonal);
        }

        double median(double[][] X) {
            double[] z = subtract(sqrt_T, normRow(X));
            return sz.median(z);
        }

    }

    static class SelfJoin_lowerBound {

        private double[] sqrt_T;
        private double[][] E;
        private SafezoneQuorum sz;

        /**
         * The safe zone function for the lower bound on the self-join estimate of an AGMS sketch.
         * The overall safe zone is defined as the median quorum over these values.
         *
         * @param E       The reference vector
         * @param T       The threshold value
         * @param eikonal The eikonality flag
         */
        SelfJoin_lowerBound(double[][] E, double T, boolean eikonal) {
            this.sqrt_T = new double[E.length];
            Arrays.fill(this.sqrt_T, (T > 0.0) ? Math.sqrt(T) : 0.0);

            if (sqrt_T[0] > 0.0) {
                double[] dest = sqrt(dotProduct(E, E)); //todo: reverse order
                sz = new SafezoneQuorum(subtract(dest, sqrt_T), (E.length + 1) / 2, eikonal);

                // normalize E
                this.E = normalize(E, dest);
            }
            //else the function returns +Infinity
        }

        double median(double[][] X) {
            if (this.sqrt_T[0] == 0.0) return Double.POSITIVE_INFINITY;
            double[] z = subtract(dotProduct(X, E), sqrt_T);
            return sz.median(z);
        }
    }

    /***** SelfJoinAGMS class *****/

    private final SelfJoin_lowerBound lowerBound;     // safezone for sk^2 >= TLow
    private final SelfJoin_upperBound upperBound;     // safezone for sk^2 <= THigh

    /**
     * The self-join safezone constructor for AGMS sketches.
     * @param E The reference vector
     * @param TLow  Lower threshold
     * @param THigh Upper threshold
     * @param eikonal   eikonality flag for computation of zeta
     */
    public SelfJoinAGMS(double[][] E, double TLow, double THigh, boolean eikonal) {
        lowerBound = new SelfJoin_lowerBound(E, TLow, eikonal);
        upperBound = new SelfJoin_upperBound(E, THigh, eikonal);

        assert TLow < THigh;
    }

    /**
     * 	Safezone for the condition  TLow <= dotProduct(X) <= THigh.
     *
     * 	This is essentially a wrapper for two safezones,
     * 	one for upper bound and one for lower bound.
     * @param X The drift vector
     * @return  The maximum of the two safezone values
     */
    public double zeta(double[][] X) {
        return min(lowerBound.median(X), upperBound.median(X));
    }
}