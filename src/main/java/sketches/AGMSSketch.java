package sketches;

import java.util.Arrays;

import static utils.SketchMath.seedVector;

public class AGMSSketch {
    private Double[][] sketchVector;

    public AGMSSketch(int depth, int width){
        sketchVector = new Double[depth][width];
        for(Double[] row : sketchVector)
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

    public int sumSquares(int d) {
        int sum = 0;
        for(int i = 0; i < this.width(); i++)
            sum += sketchVector[d][i]*sketchVector[d][i];
        return sum;
    }

    public Double elementAt(int i, int j) {
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

    public Double[][] values() {
        return sketchVector;
    }

    public Double[] getSketchColumn(int w) {
        Double[] ret = new Double[this.depth()];
        for(int d = 0; d < this.depth(); d++)
            ret[d] = sketchVector[d][w];
        return ret;
    }

    public void setSketchColumn(int w, Double[] column) {
        for(int d = 0; d < this.depth(); d++)
            this.sketchVector[d][w] = column[d];
    }

    @Override
    public String toString() {
        return Arrays.deepToString(this.sketchVector);
    }

}
