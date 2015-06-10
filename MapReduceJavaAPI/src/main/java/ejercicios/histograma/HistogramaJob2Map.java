package ejercicios.histograma;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class HistogramaJob2Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
	
	public static final String DEFAULT_FS = "hdfs://quickstart.cloudera:8020/";
	
	Double minValue;
	Double maxValue;
	
	String numColumnas;
	

	protected void setup(Context context) throws IOException, InterruptedException {

		// Se obtienen los valores pasados por contexto
		numColumnas = context.getConfiguration().get("numColumnas");
		String ficheroOutJob1 = context.getConfiguration().get("ficheroOutJob1");
		
		
		String contenimdoFichero = "";
		
		FileSystem fileSystemHDFS = null;
		try {
			fileSystemHDFS = FileSystem.get(new URI(DEFAULT_FS), context.getConfiguration());
		} catch (URISyntaxException e1) {
			e1.printStackTrace();
		}
		   
        // Se generan los objtos Path con los argumentos de entrada
        Path fileIn = new Path(ficheroOutJob1); 
        
        // Se obtiene el/los fichero/s mediante el patron del argumento de entrada
        FileStatus[] fileStatus = fileSystemHDFS.globStatus(fileIn);
                   
        // Se recorre el array de los fichos obgenidos del fileSystem de hadoop
        for(FileStatus fs : fileStatus){
       
        	// Se obtiene el FileStatus
            byte[] content = new byte[(int)fileSystemHDFS.getFileStatus(fs.getPath()).getLen()];
           
            // Me diante un InputStream del fileSystem de hadoop se lee el contenido del fichero
            FSDataInputStream fSDataInputStream = fileSystemHDFS.open(fs.getPath());
            fSDataInputStream.readFully(content);
            fSDataInputStream.close();
  
            // se agrega el contenido de cada fichero en el ficchero secuencial con su clave, valor correspondiente
            contenimdoFichero = new String(content);
           
        }
		
		String[] maxMinValues = contenimdoFichero.split(" ");
		String[] minimoValue = maxMinValues[0].split("\t");
		
		minValue = Double.parseDouble(minimoValue[1]);
		maxValue = Double.parseDouble(maxMinValues[1]);
		
	}

	
	@Override
	protected void map(LongWritable offset, Text numero, Context context) throws IOException ,InterruptedException {

		Double numDouble = Double.parseDouble(numero.toString());
		Double numCol = Double.parseDouble(numColumnas);
		
		double barDouble = (numDouble - minValue) / ((maxValue - minValue) / numCol);

		// Se redondea a numero entero
		Integer barInt = (int) barDouble;

		if(numDouble.equals(maxValue)){
			context.write(new IntWritable(barInt - 1), new IntWritable(1));
		}else{
			context.write(new IntWritable(barInt), new IntWritable(1));
		}
		
	};
	
}