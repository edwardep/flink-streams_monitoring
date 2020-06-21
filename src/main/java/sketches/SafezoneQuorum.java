package sketches;


import java.util.Arrays;

import static sketches.SketchMath.multiply;


/**
 Safezone for boolean quorum queries.

 A \f$(n,k)\f$-quorum boolean function is the boolean function
 which is true if and only if \f$k\f$ or more of its
 inputs are true. In particular:
 * For \f$k=1\f$ this function is the logical OR.
 * For \f$k=n\f$ it is the logical AND.
 * For \f$k=(n+1)/2\f$, it is the majority function.

 This implementation computes an expensive version of the
 safe zone function. In particular, it preserves eikonality
 and non-redundancy of the inputs.

 Because of its complexity, evaluating this function is expensive:
 each invocation takes time \f$O(\binom{l}{l-k+1})\f$ which can
 be quite high (\f$l \leq n \f$ is the number of true components
 in the original estimate vector, or, equivalently, the number
 of positive elements of vector \f$zE\f$ passed at construction).

 Because it is so expensive for large \f$n\f$, a fast safe zone
 is also available. Its advantage is that it is quite efficient:
 each call takes \f$O(l)\f$ time.
 Its drawback is that it is not eikonal.

 */
public class SafezoneQuorum {

    private int n;              // the number of inputs
    private int k;              // the lower bound on true inputs
    private int[] L;            // the legal inputs index from n to zetaE
    private Double[] zetaE;     // the reference vector's zetas, where zE >= 0
    private Double[] zCached;   // caching coefficients for faster execution
    private boolean eikonal = true;

    public SafezoneQuorum() {}
    public SafezoneQuorum(Double[] zE, int k, boolean eik) {
        prepare(zE, k);
        setEikonal(eik);
    }

    public void setEikonal(boolean eikonal) { this.eikonal = eikonal; }

    public void prepare(Double[] zE, int k) {
        this.n = zE.length;
        this.k = k;

        // count legal inputs
        int[] Legal = new int[n];
        int pos = 0;
        for(int i=0; i < n; i++)
            if (zE[i] > 0) Legal[pos++] = i;

        // create the index and the other matrices
        L = Arrays.copyOfRange(Legal,0, pos);
        zetaE = new Double[pos];

        int iter = 0;
        for(int index : L) zetaE[iter++] = zE[index];


        assert 1 <= k && k <= n;
        if(L.length < k)
            throw new IllegalArgumentException("The reference vector is non-admissible:"+ Arrays.toString(zE));
    }

    /**
     *  This recursion is used to compute zinf, when the cached array is just $\zeta(E_i)^2$.
     *  The recursion performs 2C additions, C divisions, C square roots and C comparisons,
     *  where C = (l choose m)
     */
    private double find_min(int m, int l, int b, Double[] zEzX, Double[] zE2, double SzEzX, double SzE2) {
        if(m==0) return SzEzX/Math.sqrt(SzE2);

        double zinf = find_min(m-1, l, b+1, zEzX, zE2, SzEzX + zEzX[b], SzE2 + zE2[b]);

        int c = l-m+1;
        for(int i = b+1; i < c; i++){
            double zi = find_min(m-1, l, i+1, zEzX, zE2, SzEzX + zEzX[i], SzE2 + zE2[i]);
            zinf = Math.min(zinf, zi);
        }
        return zinf;
    }

    private double zetaEikonal(Double[] zX) {
        // pre-compute zeta_i(E)*zeta_i(X) for all i in L
        Double[] zEzX = zetaE.clone();

        Double[] zXL = new Double[L.length];
        int iter = 0;
        for(int index : L) zXL[iter++] = zX[index];

        zEzX = multiply(zEzX, zXL);

        if(zCached == null)
            prepareZCache();

        int l = L.length;
        int m = l - k + 1;

        // selecting find_min by default because find_min_cached is not implemented yet
        return find_min(m, l, 0, zEzX, zCached, 0d,0d);
    }

    private double zetaNonEikonal(Double[] zX) {
        int iter = 0;
        Double[] zEzX = zetaE.clone();
        Double[] zXL = new Double[L.length];
        for(int index : L) zXL[iter++] = zX[index];
        zEzX = multiply(zEzX, zXL);

        Arrays.sort(zEzX);
        double res = 0d;
        for(int i=0; i < L.length-k+1; i++)
            res += zEzX[i];

        return res;
    }

    // This version just caches $\zeta(E_i)^2$ (saving us some multiplications)
    public void prepareZCache() {
        this.zCached = new Double[zetaE.length];
        this.zCached = Arrays.copyOf(multiply(zetaE, zetaE), zetaE.length);
    }

    public double median(Double[] zX) {
        return eikonal ? zetaEikonal(zX) : zetaNonEikonal(zX);
    }

    public int getN() {
        return n;
    }

    public int getK() {
        return k;
    }

    public int[] getL() {
        return L;
    }

    public Double[] getZetaE() {
        return zetaE;
    }

    public Double[] getzCached() {
        return zCached;
    }
}
