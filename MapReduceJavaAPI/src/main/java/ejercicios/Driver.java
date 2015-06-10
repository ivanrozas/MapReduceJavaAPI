package ejercicios;


import org.apache.hadoop.util.ProgramDriver;

import ejercicios.amigos.hadoop.AmigosJobHadoop;
import ejercicios.amigos.pangool.AmigosJobPangool;
import ejercicios.histograma.HistogramaJob1Hadoop;
import ejercicios.histograma.HistogramaJob2Hadoop;
import ejercicios.histograma.HistogramaJobFlow;


public class Driver extends ProgramDriver {

	public Driver() throws Throwable {
		super();

		addClass("Histograma-hadoop-flow", HistogramaJobFlow.class, "Just write the input as output (text files)");
		addClass("Histograma-hadoop-Job1", HistogramaJob1Hadoop.class, "Just write the input as output (text files)");
		addClass("Histograma-hadoop-Job2", HistogramaJob2Hadoop.class, "Just write the input as output (text files)");
		addClass("Amigos-hadoop", AmigosJobHadoop.class, "Just write the input as output (text files)");
		addClass("Amigos-pangool", AmigosJobPangool.class, "Just write the input as output (text files)");
	}

	public static void main(String[] args) throws Throwable {
		Driver driver = new Driver();
		driver.driver(args);
		System.exit(0);
	}
}
