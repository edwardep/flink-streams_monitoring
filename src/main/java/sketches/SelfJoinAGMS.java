package sketches;

import fgm.SafeZone;

import java.io.Serializable;
import java.util.Arrays;

import static java.lang.Math.min;
import static sketches.SketchMath.*;

public class SelfJoinAGMS extends SafeZone {

    static class SelfJoinUpperBound implements Serializable {
        private double[] sqrt_T;
        private SafezoneQuorum sz;


        /** The safe zone function for the upper bound on the self-join estimate of an AGMS sketch.
        *   The overall safe zone is defined as the median quorum over these values.
        *   @param E The reference vector
        *   @param T The threshold value
        *   @param eikonal The eikonality flag
        */


        SelfJoinUpperBound(double[][] E, double T, boolean eikonal) {
            this.sqrt_T = new double[E.length];
            Arrays.fill(this.sqrt_T, Math.sqrt(T));

            double[] dest = subtract(sqrt_T, normRow(E));
            sz = new SafezoneQuorum(dest, (E.length+1)/2, eikonal);
        }

        double median(double[][] X) {
            double[] z = subtract(sqrt_T, normRow(X));
            return sz.median(z);
        }

        /**
         * REQUIRED for POJO type serialization
         */
        public SelfJoinUpperBound() {
        }

        public double[] getSqrt_T() {
            return sqrt_T;
        }

        public void setSqrt_T(double[] sqrt_T) {
            this.sqrt_T = sqrt_T;
        }

        public SafezoneQuorum getSz() {
            return sz;
        }

        public void setSz(SafezoneQuorum sz) {
            this.sz = sz;
        }
    }

    static class SelfJoinLowerBound implements Serializable{

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
        SelfJoinLowerBound(double[][] E, double T, boolean eikonal) {
            this.sqrt_T = new double[E.length];
            Arrays.fill(this.sqrt_T, (T > 0.0) ? Math.sqrt(T) : 0.0);

            if (sqrt_T[0] > 0.0) {
                double[] dest = sqrt(dotProduct(E, E));
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

        /**
         * REQUIRED for POJO type serialization
         */
        public SelfJoinLowerBound() {
        }

        public double[] getSqrt_T() {
            return sqrt_T;
        }

        public void setSqrt_T(double[] sqrt_T) {
            this.sqrt_T = sqrt_T;
        }

        public double[][] getE() {
            return E;
        }

        public void setE(double[][] e) {
            E = e;
        }

        public SafezoneQuorum getSz() {
            return sz;
        }

        public void setSz(SafezoneQuorum sz) {
            this.sz = sz;
        }
    }

    /***** SelfJoinAGMS class *****/

    private SelfJoinLowerBound lowerBound;     // safezone for sk^2 >= TLow
    private SelfJoinUpperBound upperBound;     // safezone for sk^2 <= THigh


    /**
     * The self-join safezone constructor for AGMS sketches.
     * @param E The reference vector
     * @param TLow  Lower threshold
     * @param THigh Upper threshold
     * @param eikonal   eikonality flag for computation of zeta
     */
    public SelfJoinAGMS(double[][] E, double TLow, double THigh, boolean eikonal) {
        lowerBound = new SelfJoinLowerBound(E, TLow, eikonal);
        upperBound = new SelfJoinUpperBound(E, THigh, eikonal);

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


    /**
     * _______________________________________________________________________________________
     * WARNING: The empty constructor and get & set methods are required in order for Flink to treat the objects
     * of this class as POJO when serializing. Otherwise it treats it as Generic type and falls back to KryoSerial.
     *
     * Users should call the overloaded constructor and initialize lower and upper bounds
     */
    public SelfJoinAGMS() {}

    public SelfJoinLowerBound getLowerBound() {
        return lowerBound;
    }

    public void setLowerBound(SelfJoinLowerBound lowerBound) {
        this.lowerBound = lowerBound;
    }

    public SelfJoinUpperBound getUpperBound() {
        return upperBound;
    }

    public void setUpperBound(SelfJoinUpperBound upperBound) {
        this.upperBound = upperBound;
    }
}
