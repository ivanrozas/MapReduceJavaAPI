package ejercicios.histograma;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ejercicios.writables.MaxMinDoubleWritable;

/**
 * Simple example job that writes the same records that reads but sorted (input
 * must be text)
 */
public class HistogramaJob1Hadoop extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.out.println("Invalid number of arguments\n\n" + "Usage: Histograma-hadoop-job1 <input_path> <output_path>\n\n");
			return -1;
		}
		
		
		String input = args[0];
		String output = args[1];

		// Se borra el fichero por si existe que no de fallo
		Path oPath = new Path(output);
		FileSystem.get(oPath.toUri(), getConf()).delete(oPath, true);

		Job job = Job.getInstance(getConf(), "Histograma Hadoop");
		job.setJarByClass(HistogramaJob1Hadoop.class);

		
		// input/output job
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Map
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(MaxMinDoubleWritable.class);
		
		// Reducer
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(MaxMinDoubleWritable.class);

		// Clases del Job
		job.setMapperClass(HistogramaJob1Map.class);
		job.setCombinerClass(HistogramaJob1Reducer.class);
		job.setReducerClass(HistogramaJob1Reducer.class); 	

		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		

		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String args[]) throws Exception {
		ToolRunner.run(new HistogramaJob1Hadoop(), args);
	}
}
