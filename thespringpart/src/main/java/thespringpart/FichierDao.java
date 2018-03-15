package thespringpart;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;



public interface FichierDao {
	 List<String> showContent(String path) throws UnsupportedEncodingException, FileNotFoundException;  
	 List<String> showMoreContent(String path) throws UnsupportedEncodingException, FileNotFoundException; 
	 String showStructure(String path) throws UnsupportedEncodingException, FileNotFoundException;
	 List<String>  join(String path,String field_one, String second_file,String field_two) throws FileNotFoundException;
	 List<String>  filter(String path,String field_name, String value);
	 List<String> writeToHdfs(String path) throws IllegalArgumentException, IOException;
}
