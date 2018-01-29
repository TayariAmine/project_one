package thejavapart;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.List;


public abstract class Fichier {
	private String path;
	abstract List<String> showDetails(int number_of_rows) throws UnsupportedEncodingException, FileNotFoundException;
	abstract String showStructure() throws UnsupportedEncodingException, FileNotFoundException;
	//abstract void mainProcess();

	

}
