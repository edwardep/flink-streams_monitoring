package misc;

import configurations.BaseConfig;
import org.apache.commons.rng.UniformRandomProvider;
import org.apache.commons.rng.simple.RandomSource;
import org.junit.Test;
import sketches.AGMSSketch;

import java.util.Arrays;
import java.util.Random;

public class Seed {

    @Test
    public void randomSource_seed_test() {
        AGMSSketch sk1 = new AGMSSketch(3,5);
        AGMSSketch sk2 = new AGMSSketch(3,5);

        Random rand = new Random(523);

        for(int i = 0; i < 100; i++) {
            long key = rand.nextLong();
            double val = rand.nextDouble();
            sk1.update(key, val);
            sk2.update(key, val);
        }

        for(int i = 0; i < sk1.depth(); i++){
            for(int j = 0; j < sk1.width(); j++)
                assert sk1.values()[i][j].equals(sk2.values()[i][j]);
        }
    }

    @Test
    public void sketch_SeedVector_test() {

        AGMSSketch sk1 = new AGMSSketch(9, 5);
        AGMSSketch sk2 = new AGMSSketch(3, 5);

        for(int i = 0; i < 6; i++){
            for(int j = 0; j < sk1.depth(); j++)
                assert sk1.getSeedVector()[i][j].equals(sk2.getSeedVector()[i][j]);
        }
    }

    @Test
    public void sketch_init_performance() {
        // ca. 10 sec for 1000 instances
        long start = System.currentTimeMillis();
        AGMSSketch sketch = null;
        for(int i = 0; i < 1000; i++) {
            sketch = new AGMSSketch(7, 5000);
        }
        System.out.println(sketch);
        System.out.println(System.currentTimeMillis() - start);
    }


}
