package sketches;

import java.util.Arrays;

import static sketches.SketchMath.seedVector;

public class AGMSSketch {
    private double[][] sketchVector;

    public AGMSSketch(int depth, int width){
        sketchVector = new double[depth][width];
        for(double[] row : sketchVector)
            Arrays.fill(row, 0d);
    }

    private long hash31(long a, long b, long x) {
        long result = (a * x) + b;
        return ((result >> 31) ^ result) & 2147483647;
    }

    private int hash(int d, long x) {
        assert d < this.depth();
        return (int) hash31(seedVector[0][d], seedVector[1][d], x) % this.width();
    }


    private long fourwise(long s1, long s2, long s3, long s4, long x) {
        return hash31(hash31(hash31(x, s1, s2), x, s3), x, s4) & (1 << 15);
    }

    // return a 4-wise independent number {-1,+1}
    private int fourwise(int d, long x) {
        return (fourwise(seedVector[2][d], seedVector[3][d], seedVector[4][d], seedVector[5][d], x) > 0) ? 1 : -1;
    }

    public void update(long key, double value) {
        for(int d = 0; d < depth(); d++){
            int hash = hash(d, key);
            int xi = fourwise(d, key);

            sketchVector[d][hash] += value*xi;
        }
    }


    public double elementAt(int i, int j) {
        return sketchVector[i][j];
    }

    public int depth() {
        return sketchVector.length;
    }
    public int width() {
        return sketchVector[0].length;
    }
    public int size() {
        return depth()*width();
    }

    public Long[][] getSeedVector() {
        return seedVector;
    }

    public double[][] values() {
        return sketchVector;
    }

    public double[] getSketchColumn(int w) {
        double[] ret = new double[this.depth()];
        for(int d = 0; d < this.depth(); d++)
            ret[d] = sketchVector[d][w];
        return ret;
    }

    public void setSketchColumn(int w, double[] column) {
        for(int d = 0; d < this.depth(); d++)
            this.sketchVector[d][w] = column[d];
    }

    @Override
    public String toString() {
        return Arrays.deepToString(this.sketchVector);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AGMSSketch that = (AGMSSketch) o;
        return Arrays.deepEquals(sketchVector, that.sketchVector);
    }

    @Override
    public int hashCode() {
        return Arrays.deepHashCode(sketchVector);
    }
}
