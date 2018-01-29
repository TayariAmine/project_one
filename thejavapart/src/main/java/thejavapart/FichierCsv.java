package thejavapart;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;


public class FichierCsv extends Fichier {
	private String path;
	private CsvParser parser;

	public FichierCsv(String path) {
		this.path=path;
		CsvParserSettings settings = new CsvParserSettings();
		settings.setLineSeparatorDetectionEnabled(true);
		 this.parser = new CsvParser(settings);
	}
	
	

	public List<String> showDetails(int number_of_rows) throws UnsupportedEncodingException, FileNotFoundException{
		
		List<String> result = new ArrayList<String>();
        int counter =0;
		this.parser.beginParsing(getReader(this.path));
		
		String[] row;
		while ((row = this.parser.parseNext()) != null & (counter<= number_of_rows -1)) {
			result.add(Arrays.toString(row));
			counter =counter +1;
		}

		this.parser.stopParsing();
		
		return result;
	}
	public Reader getReader(String relativePath) throws UnsupportedEncodingException, FileNotFoundException {
		
		return new InputStreamReader(new FileInputStream(this.path), "UTF-8");
		
	}
	public String showStructure() throws UnsupportedEncodingException, FileNotFoundException{
		this.parser.beginParsing(getReader(this.path));
		boolean duplicates =false;
		boolean numerique = false;
		boolean date = false;
		int j;
		int k;
				
		String[] row = this.parser.parseNext();
		String result="";
	
		  
		for (j=0;j<row.length;j++){
			numerique= numerique || NumberUtils.isNumber(row[j]);
			date =date || row[j].matches("([0-9]{2})/([0-9]{2})/([0-9]{4})") 
					|| row[j].matches("([0-9]{2})-([0-9]{2})-([0-9]{4})")
					|| row[j].matches("([0-9]{2})/([0-9]{2})/([0-9]{2})")
					|| row[j].matches("([0-9]{2})-([0-9]{2})-([0-9]{2})");
			for (k=j+1;k<row.length;k++){
			    if (k!=j && row[k] == row[j])
			      duplicates=true;
			}
		}
					
		if (duplicates || numerique || date){
			result="this file has "+row.length+" columns but no header" ;
			
		}
		else{
			String columns ="his file has "+row.length+" columns";
			
			for (j=0;j<row.length;j++){
				columns=columns +"\n"+"-"+row[j];
			}
			result=columns;
		}
		
		return result;
		
	
	}
	public List<String> joinWith(final int field_in_this, final String path_to_join, final int field_in_second ) throws FileNotFoundException{
		// TODO Auto-generated method stub
		List<String> result =new ArrayList<String>();
		try (Stream<String> stream = Files.lines(Paths.get(path))) {
		    stream.map(line->{
				
			String value_to_join_on=line.split(";")[field_in_this];
			
			try (Stream<String> stream2 = Files.lines(Paths.get(path_to_join))){
				stream2
				.filter(line_in_second->line_in_second.split(";")[field_in_second].equals(value_to_join_on))
				.map(line_in_second->line_in_second+";"+line)
					
			.forEach(s->result.add(s));
			
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		
			
			return "";
		
			}).forEach(System.out::print);
			
        
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;  
		    
	}
	public String copyToHDFS() throws IllegalArgumentException, IOException {

		String uri = "hdfs://192.168.1.111:8020";
		URI hdfsuri = URI.create(uri);

		// ====== Init HDFS File System Object
		Configuration conf = new Configuration();

		// Set FileSystem URI
		conf.set("fs.defaultFS", uri);
		// Because of Maven
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		// Set HADOOP user
		System.setProperty("HADOOP_USER_NAME", "hdfs");
		System.setProperty("hadoop.home.dir", "/");
		// Get the filesystem - HDFS
		FileSystem fs = FileSystem.get(hdfsuri, conf);
		
		fs.copyFromLocalFile(new Path(this.path), new Path("hdfs://192.168.1.111:8020/user/emira/data"));
         
		return ("fichier transféré");
	}


	
}
