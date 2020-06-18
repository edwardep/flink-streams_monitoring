package sketches;

import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static junit.framework.TestCase.assertEquals;
import static test_utils.Generators.uniform_random_vector;
import static utils.SketchOperators.multiply;
import static utils.SketchOperators.norm;

public class SafezoneQuorum_test {
    /**
     * Testing function of the quantile safezone, eikonal and non-eikonal
     */
    @Test
    public void test_quorum_est() {
        Double[] zE = {13.0, 17.0, 26.0, 11.0, -33.0, 31.0, 52.0};

        // Test that the eikonal and non-eikonal safe zones are equal
        SafezoneQuorum sze = new SafezoneQuorum(zE, (zE.length+1)/2, true);
        assertEquals(7, sze.getN());
        assertEquals(4, sze.getK());
        assertEquals(6,sze.getL().length);

        SafezoneQuorum szne = new SafezoneQuorum(zE, (zE.length+1)/2, false);
        assertEquals(7, szne.getN());
        assertEquals(4, szne.getK());
        assertEquals(6,szne.getL().length);

        for(int i=0;i<10000;i++){
            Double[] zX = uniform_random_vector(zE.length, 100, -50);

            double we = sze.median(zX);
            double wne = szne.median(zX);

            assertEquals("at "+i,we >= 0, wne >= 0);
        }
    }

    /**
     * Test that k=N produces the minimum function
     */
    @Test
    public void test_quorum_AND_case() {
        int N=7;

        // run 10 tests
        for(int i=0; i<10; i++) {
            // produce a random reference point
            Double[] E = uniform_random_vector(N, 9.9, 0.1);
            SafezoneQuorum sz = new SafezoneQuorum(E, N, true);
            SafezoneQuorum szf = new SafezoneQuorum(E, N, false);

            // test 100 vectors
            for(int j=0; j<100; j++) {
                Double[] z = uniform_random_vector(N, 40, -20);

                double med_e = sz.median(z);
                double med_ne = szf.median(z);
                Arrays.sort(z);
                double min = z[0];

                assertEquals(med_e, min, 1e-10);
                //fixme: non-eikonal testing needs rework
//                assertEquals(med_ne, min, 1e-10);
            }
        }
    }

    /**
     * Test that k=1 produces the OR function
     */
    @Test
    public void test_quorum_OR_case() {
        int N=7;

        // run 10 tests
        for(int i=0; i<1000; i++) {
            // produce a random reference point
            Double[] E = uniform_random_vector(N, 9.9, 0.1);

            SafezoneQuorum sz = new SafezoneQuorum(E, 1, true);
            SafezoneQuorum szf = new SafezoneQuorum(E,1, false);

            double ENorm = norm(E);

            // test 100 vectors
            for(int j=0; j<100; j++) {
                Double[] z = uniform_random_vector(N, 40, -20);

                double sum = Arrays.stream(multiply(z,E)).reduce(0d, Double::sum);

                assertEquals(sum/ENorm, sz.median(z), 1e-10);
                assertEquals(sum, szf.median(z), 1e-10);
            }
        }
    }


    @Test
    public void prepareZ_cache_cheap() {
        Double[] zE = new Double[7];
        Random rand = new Random();
        for(int i = 0; i < 7; i++)
            zE[i] = rand.nextDouble() - .5;

        SafezoneQuorum quorum = new SafezoneQuorum(zE, 2,false);

        quorum.prepareZCache();

        for(int i = 0; i < quorum.getL().length; i++)
            assert quorum.getZetaE()[i]*quorum.getZetaE()[i] == quorum.getzCached()[i];
    }

    @Test
    public void quorum_prepare() {

        Double[] zE = new Double[7];
        Random rand = new Random();
        for(int i = 0; i < 7; i++)
            zE[i] = rand.nextDouble() - .5;

        SafezoneQuorum quorum = new SafezoneQuorum(zE, 2,false);

        System.out.println("zE: "+Arrays.toString(zE));
        System.out.println("L: "+Arrays.toString(quorum.getL()));
        System.out.println("zetaE: "+Arrays.toString(quorum.getZetaE()));
    }

    @Test
    public void non_eikonal_cpp_comparison() {
        Double[] zEzX = {3.0,2.0,1.0,4.0,5.0,6.0,7.0,8.0,9.0};
        Double[] zX = {1d,2d,3d,4d,5d,6d,7d,8d,9d};

        for(int i = 1; i <= 8; i++) {
            SafezoneQuorum sz = new SafezoneQuorum(zEzX, i, false);
            System.out.println("k: "+i+", sum: "+sz.median(zX));
        }
    }
}
