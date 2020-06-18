package sketches;

import org.junit.Test;

import static java.lang.Math.sqrt;
import static junit.framework.TestCase.assertEquals;

public class Bilinear2d_safezone_test {

    @Test
    public void test_hyperbola_nn_0()
    {
        Bilinear2d_safezone b2d = new Bilinear2d_safezone();

        assertEquals( b2d.hyperbola_nearest_neighbor(0.0,0.0,0.0), 0.0);

        assertEquals( b2d.hyperbola_nearest_neighbor(1.0,-3.0,0.0), 0.0, 1E-9);
        assertEquals( b2d.hyperbola_nearest_neighbor(1.0,-1.0,0.0), 0.0, 1E-9);
        assertEquals( b2d.hyperbola_nearest_neighbor(1.0,-0.9,0.0), 0.05, 1E-9);
        assertEquals( b2d.hyperbola_nearest_neighbor(1.0,0.0,0.0), 0.5, 1E-9);
        assertEquals( b2d.hyperbola_nearest_neighbor(1.0,1.0,0.0), 1.0, 1E-9);
        assertEquals( b2d.hyperbola_nearest_neighbor(1.0,2.0,0.0), 1.5, 1E-9);

        assertEquals( b2d.hyperbola_nearest_neighbor(-1.0,-3.0,0.0), 0.0, 1E-9);
        assertEquals( b2d.hyperbola_nearest_neighbor(-1.0,-1.0,0.0), 0.0, 1E-9);
        assertEquals( b2d.hyperbola_nearest_neighbor(-1.0,-0.9,0.0), -0.05, 1E-9);
        assertEquals( b2d.hyperbola_nearest_neighbor(-1.0,0.0,0.0), -0.5, 1E-9);
        assertEquals( b2d.hyperbola_nearest_neighbor(-1.0,1.0,0.0), -1.0, 1E-9);
        assertEquals( b2d.hyperbola_nearest_neighbor(-1.0,2.0,0.0), -1.5, 1E-9);

        assertEquals( b2d.hyperbola_nearest_neighbor(0.0,1.0,0.0), 0.5, 1E-9);
        assertEquals( b2d.hyperbola_nearest_neighbor(0.0,10.0,0.0), 5.0, 1E-9);
        assertEquals( b2d.hyperbola_nearest_neighbor(0.0,-1.0,0.0), 0.0, 1E-9);
    }

    @Test
    public 	void test_hyperbola_nn_scale()
    {
        Bilinear2d_safezone b2b = new Bilinear2d_safezone();

        for(double xi=-10; xi<=10; xi+=0.11) {
            double psi = sqrt(xi*xi+1.0);
            for(double t=0.49; t>=-100.0; t+=((t>=-1.0)?-0.01:t) ) {
                double p = xi-2.0*t*xi;
                double q = psi + 2.0*t*psi;

                double xi_ret = b2b.hyperbola_nearest_neighbor(p,q,1.0);

                assertEquals( xi_ret, xi, 1E-9);

                //System.out.println("p="+p+" q="+q+" xi="+xi+"xi_ret="+xi_ret);

                for(double T = 1E-5; T<= 1E5; T*=10.) {
                    double sqrtT = sqrt(T);
                    double xi_s = b2b.hyperbola_nearest_neighbor(p*sqrtT, q*sqrtT, T)/sqrtT;
                    assertEquals( xi_s, xi_ret, 1E-9 );
                }
            }
        }
    }


    @Test
    public 	void test_bilinear_2d_T_zero()
    {
        Bilinear2d_safezone zeta = new Bilinear2d_safezone( 1., 0., 0.);

        assertEquals( zeta.bilinear2d(1.,0.), 1./sqrt(2), 1.E-12);
        assertEquals( zeta.bilinear2d(1.,1.), 0., 1.E-12);
        assertEquals( zeta.bilinear2d(0.,1.), -1./sqrt(2), 1.E-12);
        assertEquals( zeta.bilinear2d(-1.,0.), -1/sqrt(2), 1.E-12); //note: the SDF here would yield -1 < -1/\sqrt{2}!

        zeta = new Bilinear2d_safezone(0.,0.,0.); // this should yield the same safe zone as above
        assertEquals( zeta.bilinear2d(1.,0.), 1./sqrt(2), 1.E-12);
        assertEquals( zeta.bilinear2d(1.,1.), 0., 1.E-12);
        assertEquals( zeta.bilinear2d(0.,1.), -1./sqrt(2), 1.E-12);
        assertEquals( zeta.bilinear2d(-1.,0.), -1/sqrt(2), 1.E-12); //note: the SDF here would yield -1 < -1/\sqrt{2}!

        zeta = new Bilinear2d_safezone(1.,1.,0.); // this should yield the same safe zone as above
        assertEquals( zeta.bilinear2d(1.,0.), 1./sqrt(2), 1.E-12);
        assertEquals( zeta.bilinear2d(1.,1.), 0., 1.E-12);
        assertEquals( zeta.bilinear2d(0.,1.), -1./sqrt(2), 1.E-12);
        assertEquals( zeta.bilinear2d(-1.,0.), -1/sqrt(2), 1.E-12); //note: the SDF here would yield -1 < -1/\sqrt{2}!

        zeta = new Bilinear2d_safezone(-1.,1.,0.); // this should define the left-facing cone
        assertEquals( zeta.bilinear2d(1.,0.), -1./sqrt(2), 1.E-12);
        assertEquals( zeta.bilinear2d(1.,1.), -sqrt(2), 1.E-12);
        assertEquals( zeta.bilinear2d(0.,1.), -1./sqrt(2), 1.E-12);
        assertEquals( zeta.bilinear2d(-1.,0.), 1/sqrt(2), 1.E-12); //note: the SDF here would yield -1 < -1/\sqrt{2}!

        zeta = new Bilinear2d_safezone(-1.,0.5,0.); // this should define the left-facing cone
        assertEquals( zeta.bilinear2d(1.,0.), -1./sqrt(2), 1.E-12);
        assertEquals( zeta.bilinear2d(1.,1.), -sqrt(2), 1.E-12);
        assertEquals( zeta.bilinear2d(0.,1.), -1./sqrt(2), 1.E-12);
        assertEquals( zeta.bilinear2d(-1.,0.), 1/sqrt(2), 1.E-12); //note: the SDF here would yield -1 < -1/\sqrt{2}!

        zeta = new Bilinear2d_safezone(-1.,-0.5,0.); // this should define the left-facing cone
        assertEquals( zeta.bilinear2d(1.,0.), -1./sqrt(2), 1.E-12);
        assertEquals( zeta.bilinear2d(1.,1.), -sqrt(2), 1.E-12);
        assertEquals( zeta.bilinear2d(0.,1.), -1./sqrt(2), 1.E-12);
        assertEquals( zeta.bilinear2d(-1.,0.), 1/sqrt(2), 1.E-12); //note: the SDF here would yield -1 < -1/\sqrt{2}!
    }


    @Test
    public void test_bilinear_2d_T_pos() {
        Bilinear2d_safezone zeta = new Bilinear2d_safezone(1, 0, 1);

        assertEquals( zeta.getU(), 0.0);
        assertEquals( zeta.getV(), 0.0);
        assertEquals( zeta.getT(), 1.0);
        assertEquals( zeta.getXiHat(), 1);

        assertEquals( zeta.bilinear2d(sqrt(2),1), 0., 1E-12);
        assertEquals( zeta.bilinear2d(sqrt(5),2), 0., 1E-12);
        assertEquals( zeta.bilinear2d(sqrt(5),2), 0., 1E-12);

        for(double a=0.1; a<10; a+=0.1) {
            double d = sqrt(2*(a*a)+1);
            assertEquals( zeta.bilinear2d(0,2*a), -d, 1E-12);
            assertEquals( zeta.bilinear2d(2.0*sqrt((a*a)+1.0),0), d, 1E-12);
        }

        zeta = new Bilinear2d_safezone(-1.5, -0.5, 1);

        assertEquals( zeta.bilinear2d(-sqrt(2),1), 0., 1E-12);
        assertEquals( zeta.bilinear2d(-sqrt(5),2), 0., 1E-12);
        assertEquals( zeta.bilinear2d(-sqrt(5),2), 0., 1E-12);

        for(double a=0.1; a<10; a+=0.1) {
            double d = sqrt(2*(a*a)+1);
            assertEquals( zeta.bilinear2d(0,2*a), -d, 1E-12);
            assertEquals( zeta.bilinear2d(-2.0*sqrt((a*a)+1.0),0), d, 1E-12);
        }
    }

    @Test
    public 	void test_bilinear_2d_T_neg()
    {
        Bilinear2d_safezone zeta = new Bilinear2d_safezone(1, 0, -1);

        double uu = 0.5;
        double vv = sqrt(1.+ (uu*uu));
        double norm_uuvv = sqrt((uu*uu) + (vv*vv));
        assertEquals( zeta.getU(), uu/norm_uuvv, 1E-12);
        assertEquals( zeta.getV(), vv/norm_uuvv, 1E-12);

        assertEquals( zeta.bilinear2d(0,sqrt(5)), -sqrt(1.5), 1E-12);
        assertEquals( zeta.bilinear2d(0,-sqrt(5)), -sqrt(1.5), 1E-12);
        assertEquals( zeta.bilinear2d(0,0), 1/norm_uuvv, 1E-12);

        zeta = new Bilinear2d_safezone(0, -0.5, -1);
        assertEquals( zeta.getU(), 0.0, 1E-16);
        assertEquals( zeta.getV(), 1.0, 1E-16);
        assertEquals( zeta.bilinear2d(0,0), 1, 1E-16);
        assertEquals( zeta.bilinear2d(10,0), 1, 1E-16);
        assertEquals( zeta.bilinear2d(-100,0), 1, 1E-16);

        assertEquals( zeta.bilinear2d(1E6,1), 0, 1E-16);
        assertEquals( zeta.bilinear2d(-1E6,-2), -1, 1E-16);
    }
}
