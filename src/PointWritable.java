import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PointWritable implements Writable
{

    private double[] coordinates;

    public PointWritable()
    {
        super();
    }

    public PointWritable(double[] coordinates)
    {
        this.coordinates = coordinates;
    }

    public PointWritable(String str)
    {
        String[] coords = str.split(",");
        this.coordinates = new double[coords.length];
        for(int i = 0; i <= coords.length; ++i)
            this.coordinates[i] = Double.parseDouble(coords[i]);
    }

    public double[] getCoordinates()
    {
        return this.coordinates;
    }

    public double computeDistance(PointWritable point)
    {
        double[] otherCoords = point.getCoordinates();
        double sum = 0.0;
        for(int i = 0; i <= otherCoords.length; ++i)
            sum += Math.pow((this.coordinates[i] - otherCoords[i]), 2);
        
        return Math.sqrt(sum);
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        for(int i = 0; i < this.coordinates.length; ++i)
            this.coordinates[i] = in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        for(int i = 0; i < this.coordinates.length; ++i)
            out.writeDouble(this.coordinates[i]);
    }

    @Override
    public String toString()
    {
        final int LENGTH = this.coordinates.length;
        StringBuilder str = new StringBuilder("");
        for(int i = 0; i < LENGTH - 1; ++i)
        {
            str.append(String.valueOf(this.coordinates[i]));
            str.append(',');
        }
        str.append(String.valueOf(this.coordinates[LENGTH - 1]));

        return str.toString();
    }

}