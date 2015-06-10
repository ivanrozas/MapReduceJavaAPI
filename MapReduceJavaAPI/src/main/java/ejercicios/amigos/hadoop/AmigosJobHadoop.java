package ejercicios.amigos.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ejercicios.writables.RelacionAmigosWritable;

/**
 * 
 * @author Ivan Rozas
 *
 */
public class AmigosJobHadoop extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.out.println("Invalid number of arguments\n\n" + "Usage: Amigos-hadoop <input_path> <output_path>\n\n");
			return -1;
		}
		
		
		String input = args[0];
		String output = args[1];

		Path oPath = new Path(output);
		FileSystem.get(oPath.toUri(), getConf()).delete(oPath, true);

		Job job = Job.getInstance(getConf(), "Amigos Hadoop");
		job.setJarByClass(AmigosJobHadoop.class);
		
		// input/output job
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Map
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(RelacionAmigosWritable.class);
		
		// Reducer
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Clases del Job
		job.setMapperClass(AmigosMap.class);
		job.setReducerClass(AmigosReducer.class); 	

		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		
		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String args[]) throws Exception {
		ToolRunner.run(new AmigosJobHadoop(), args);
	}
}
