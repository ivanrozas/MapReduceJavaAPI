package ejercicios.histograma;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HistogramaJobFlow extends Configured implements Tool {

	@Override
  public int run(String[] args) throws Exception {
		
		if(args.length != 3) {
			System.out.println("Invalid number of arguments\n\n" + "Usage: Histograma-flow <input_path> <output_path> <number_colum>\n\n");
			return -1;
		}
		
		String input = args[0];
		String output = args[1];
		String numCol = args[2];

		String outputJob1 = output + "-job1";
		String outputJob2 = output + "-job2";

		ToolRunner.run(new HistogramaJob1Hadoop(), new String[]{input, outputJob1});
		ToolRunner.run(new HistogramaJob2Hadoop(), new String[]{input, outputJob1, outputJob2, numCol});		
		
	  return 0;
  }
	
	public static void main(String args[]) throws Exception {
		ToolRunner.run(new HistogramaJobFlow(), args);
	}
}
