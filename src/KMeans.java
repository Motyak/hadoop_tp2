import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeans
{
    final public static String PROP_BARY_PATH = "bary";
    final public static int ITER_MAX = 1; //on commence avec 1 pour les tests

    List<BaryWritable> readBarycenters(Configuration conf, String filename) throws IOException
    {
        Path path = new Path(conf.get(PROP_BARY_PATH) + "/" + filename);
        Reader reader = new Reader(conf, Reader.file(path));

        List<BaryWritable> res = new ArrayList<>();
        Writable key = null;
        BaryWritable val = null;

        while(reader.next(key, val))
            res.add((BaryWritable)val);
        
        reader.close();

        return res;
    }

    void recordBarycenters(Configuration conf, String filename, List<BaryWritable> barycenters) throws IOException
    {
        Path path = new Path(conf.get(PROP_BARY_PATH) + "/" + filename);
        FileSystem fs = FileSystem.get(conf);
        
        if(fs.exists(path))
            fs.delete(path, true);

        Writer writer = SequenceFile.createWriter(
            conf,
            Writer.file(path),
            Writer.keyClass(IntWritable.class),
            Writer.valueClass(BaryWritable.class)
        );

        for(BaryWritable b : barycenters)
            writer.append(new IntWritable(b.getClusterId()), b);
    }

    public static class KMeansMapper extends Mapper<LongWritable, Text, BaryWritable, PointWritable>
    {
        private List<BaryWritable> barycenters = new ArrayList<>();

        @Override
        public void map(LongWritable key, Text value, Context context)
        {   
            // les values ici sont les lignes de fichiers csv
            // Récupérer les barycentres et les ajouter à la liste en attribut du mapper

            if(value.toString().charAt(0) == 'a')
                return;

            
            

            // /* Je tokenise le texte en mots */
            // String[] tokens = value.toString().split("\\P{L}+");

            // /* Je traite les tokens et écris les paires résultats */
            // for(String str : tokens)
            //     if(!str.isBlank())
            //         context.write(
            //             new Text(str.toLowerCase()),
            //             new IntWritable(1)
            //         );
        }
    }

    public static class KMeansReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
        {
            

            // /* on calcule la somme des entiers */
            // int sum = 0;
            // for(IntWritable i : values)
            //     sum += i.get();

            // /* On écrit le résultat dans une paire */
            // context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration config = new Configuration();
        Job job = Job.getInstance(config, "KMeans program");
        job.setJarByClass(KMeans.class);
        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
