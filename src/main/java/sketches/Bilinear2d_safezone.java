package sketches;

import static java.lang.Math.*;

/**
 A safe zone for the problem \f$x^2 - y^2 \geq T\f$ in 2 dimensions.

 The reference point for this safezone is given as \f$(\xi, \psi)\f$, and
 must satisfy the condition \f$\xi^2 - \psi^2 \geq T\f$. If this is not the case,
 an \c std::invalid_argument is thrown.

 When \f$T=0\f$, there is ambiguity if \f$ \xi = 0\f$: there are two candidate safe
 zones, one is \f$ Z_{+} =  \{ x\geq |y| \} \f$ and the other is \f$ Z_{-} = \{ x \leq -|y| \} \f$. In
 this case, the function will select zone \f$ Z_{+} \f$.

 */
public class Bilinear2d_safezone {
    private double epsilon = 1e-13;     // accuracy for hyperbola distance
    private double T;                   // threshold
    private int xiHat;                  // cached for case T>0
    private double u,v;                 // cached for case T<=0


    /**
     * Default construct an invalid safe zone
     */
    public Bilinear2d_safezone() {}

    /**
     * Construct a valid safe zone.
     * @param xi    the x-coordinate of reference point (xi, psi)
     * @param psi   the y-coordinate of reference point (xi, psi)
     * @param _T    the safe zone threshold
     */
    public Bilinear2d_safezone(double xi, double psi, double _T) {
        this.T = _T;
        this.xiHat = xi >= 0.0 ? 1 :-1;
        this.u = 0d;
        this.v = 0d;

        // cache the conic safe zone, if applicable
        if(T < 0) {
            u = hyperbola_nearest_neighbor(xi, abs(psi), -T, epsilon);
            v = sqrt(u*u -T);
            //eikonalize
            double norm_u_v = sqrt(u*u + v*v);
            assert norm_u_v > 0;
            u /= norm_u_v;
            v /= norm_u_v;
            T /= norm_u_v;
        }
        else if(T==0) {
            u = (xi >= 0.0) ? 1.0/sqrt(2.0) : -1.0/sqrt(2.0);
            v = 1.0/sqrt(2.0);
        }
    }

    /**
     *  The value of the safe zone function at (x,y)
     * @param x
     * @param y
     */
    public double bilinear2d(double x, double y) {
        if(T > 0) {
            // compute the signed distance function of the set $\{ x >= \sqrt{y^2+T} \}$.
            double x_xihat = x*xiHat;

            int sgn_delta = (x_xihat - sqrt(y*y + T) >= 0) ? 1 : -1;

            double v = hyperbola_nearest_neighbor(y, x_xihat, T, epsilon);
            double u = sqrt(v*v + T);

            return sgn_delta*sqrt((x_xihat - u)*(x_xihat - u) + (y - v)*(y - v));
        }
        else {
            return u*x - v*abs(y) - T;
        }
    }

    /**
     Consider the hyperbola \( y(x) = \sqrt{x^2+T} \), where \(T \geq 0\),
     for \( x\in [0,+\infty) \), and
     let \( (\xi, \psi) \) be a point on it.

     First, assume \( \xi >0 \). Then, the normal to the hyperbola at that point
     is the line passing through points \( (2\xi,0) \) and \( (0,2\psi) \). To see this,
     note that \( \nabla (y^2-x^2) = (-2x, 2y) \). In other words, the normal to the
     hyperbola at \( (\xi, \psi) \) has a parametric equation of
     \[  (\xi,\psi) + t (-2\xi, 2\psi).  \]

     Now, any point \( (p,q)\) whose nearest neighbor is \( (\xi, \psi) \) must satisfy
     \[  (p,q) =  (\xi,\psi) + t (-2\xi, 2\psi),  \]
     and by eliminating \( t\) we get
     \[   \frac{p}{\xi} + \frac{q}{\psi} = 2.  \]
     Thus, it suffices to find the root of the function
     \[  g(x) = 2 - p/x - q/y(x) \]
     (which is unique).

     This function has \( g(0) = -\infty\), \(g(\xi) = 0\) and \(g(\max(p,q))>0\). Also,
     \[  g'(x) = \frac{p}{x^2} + \frac{qx}{2y^3}.  \]

     In case \(p=0\), it is easy to see that, if \(q>2\sqrt{T}\), then the nearest point is
     \( (\sqrt{(q/2)^2 - T}, q/2)\), and for \( q\leq 2\sqrt{T} \), the answer is \( (0, \sqrt{T}) \).
     */
    private double hyperbola_nearest_neighbor(double p, double q, double T, double epsilon) {
        if(T < 0.0)
            throw new IllegalArgumentException("call to hyperbola_nearest_neighbor with T < 0");

        if(T == 0.0) {
            //Direct solution
            if(p < 0.0)
                return (q <= p) ? 0.0 : 0.5*(p-q);
            else
                return (q <= -p) ? 0.0 : 0.5*(p+q);
        }
        if(p == 0.0) {
            if(q > 2.*sqrt(T))
                return sqrt((q/2)*(q/2)-T);
            else
                return 0.;
        }
        if(q == 0.0)
            return p/2.;

        return bisection(p,q,T,epsilon);
    }

    public double hyperbola_nearest_neighbor(double p, double q, double T) {
        return hyperbola_nearest_neighbor(p, q, T, epsilon);
    }

    /*
        Precondition: T>0  and  p,q != 0.0
        Bisection up to 50 iterations and 10^-13 precision (by default)
    */
    private double bisection(double p, double q, double T, double epsilon) {
        // find upper and lower bounds for root
        double x0 = copySign(abs(p)/(2.5+abs(q)/sqrt(T)), p);
        assert g(x0,p,q,T) < 0;
        double x1 = copySign(max(abs(p),q), p);
        assert g(x1,p,q,T) > 0;
        double xm = (x0+x1)/2;

        int loops = 50;
        while(abs((x1-x0)/xm) >= epsilon) {
            if((--loops) == 0) break;
            double gx = g(xm,p,q,T);
            if(gx > 0)
                x1 = xm;
            else if(gx < 0)
                x0 = xm;
            else
                break;
            xm = (x0+x1)/2;
        }
        return xm;
    }

    private double g(double x, double p, double q, double T) { return 2. - p/x - q/sqrt(x*x + T); }

    public double getT() {
        return T;
    }

    public int getXiHat() {
        return xiHat;
    }

    public double getU() {
        return u;
    }

    public double getV() {
        return v;
    }
}
