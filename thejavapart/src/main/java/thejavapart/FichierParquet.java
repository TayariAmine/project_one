package thejavapart;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.*;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.GroupType;

import org.apache.parquet.schema.Type;

import java.io.IOException;

public class FichierParquet extends Fichier {
	private String path;

	public FichierParquet(String path) {
		this.path =path;
	}

	@Override
	
	public List<String> showDetails(int number_of_rows) throws UnsupportedEncodingException, FileNotFoundException {
		Path file = new Path(this.path);
		Configuration configuration = new Configuration();
		List<String> result = new ArrayList<String>();

		try {

			ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(configuration)
					.build();
			int counter = 0;
			Group group = null;

			group = reader.read();

			while ((group = reader.read()) != null & (counter <= number_of_rows - 1)) {
				counter = counter + 1;
				result.add("row " + counter + " : \n" + group);
			}

			reader.close();

		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return result;

	}

	@Override
	public String showStructure() throws UnsupportedEncodingException, FileNotFoundException {
		Path file = new Path(this.path);
		Configuration configuration = new Configuration();
		String result = "";

		try {

			ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(configuration)
					.build();

			Group group = null;

			group = reader.read();

			GroupType type = group.getType();

			int number_of_fields = type.getFieldCount();
			String fields = "";

			for (Type t : type.getFields()) {

				fields = fields + "-" + t.getName() + "\n";

			}

			result = "this file has " + number_of_fields + " columns : \n" + fields;
			reader.close();

		} catch (IOException ioe) {
			ioe.printStackTrace();
		}

		return result;
	}

	public List<String> filter(String field_name, String value) {
		Path file = new Path(this.path);
		Configuration configuration = new Configuration();
		List<String> result = new ArrayList<String>();
		try {

			ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(configuration)
					.build();

			Group group = null;
			group = reader.read();
			int index = group.getType().getFieldIndex(field_name);

			while ((group = reader.read()) != null) {
				if (group.getValueToString(index, 0).equals(value)) {
					result.add("---------"+"\n"+group);
				}
			}

			reader.close();

		} catch (IOException ioe) {
			ioe.printStackTrace();
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
