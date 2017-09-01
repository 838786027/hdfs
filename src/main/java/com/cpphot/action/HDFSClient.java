package com.cpphot.action;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HDFSClient {
	private FileSystem fileSystem;

	/**
	 * 在创建对象时，把fileSystem实例化。
	 * 
	 * @param conf
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public HDFSClient(Configuration conf) throws IOException,
			URISyntaxException {
		fileSystem = FileSystem.get(conf);
	}

	public void close() throws IOException {
		fileSystem.close();
	}

	/**
	 * 实现的命令： hadoop fs -ls /chris
	 * 
	 * @param folder
	 * @throws IOException
	 */
	public void ls(String folder) throws IOException {
		Path path = new Path(folder);
		FileStatus[] fileStatus = fileSystem.listStatus(path);
		System.out
				.println("====================================================");
		for (FileStatus fs : fileStatus) {
			System.out.println("name: " + fs.getPath() + " folder: "
					+ fs.isDir() + " size: " + fs.getLen() + " permission: ");
		}
		System.out
				.println("====================================================");
	}

	/**
	 * 实现的命令： hadoop fs -mkdir /chris/client
	 * 
	 * @param folder
	 * @throws IOException
	 */
	public void mkdir(String folder) throws IOException {
		Path path = new Path(folder);
		if (!fileSystem.exists(path)) {
			fileSystem.mkdirs(path);
			System.out.println("Created " + folder);
		}
	}

	/**
	 * 实现的命令： haoop fs -rmr /chris/client
	 * 
	 * @param folder
	 * @throws IOException
	 */
	public void rmr(String folder) throws IOException {
		Path path = new Path(folder);
		fileSystem.deleteOnExit(path);
		System.out.println("Delete the " + folder);
	}

	/**
	 * 实现的命令： hadoop fs -copyFromLocal /home/chris/test /chris/
	 * 注意：此处由于ubuntu操作系统是安装在win7的虚拟机上的，而这段程序是在win7下run的 所以此处的本地路径就是win7的。
	 * 
	 * @param local
	 * @param remote
	 * @throws IOException
	 */
	public void copyFile(String local, String remote) throws IOException {
		fileSystem.copyFromLocalFile(new Path(local), new Path(remote));
		System.out.println("Copy from " + local + " to " + remote);
	}

	/**
	 * 实现命令： hadoop fs -cat /chris/test
	 * 
	 * @param file
	 * @throws IOException
	 */
	public void cat(String file) throws IOException {
		Path path = new Path(file);
		FSDataInputStream in = fileSystem.open(path);
		IOUtils.copyBytes(in, System.out, 4096, false);
		IOUtils.closeStream(in);
	}

	/**
	 * 实现的命令： hadoop fs -copyToLocal /tmp/core-site.xml /home/chris
	 * 注意：此处的本地也是win7的路径，理由同上。
	 * 
	 * @param remote
	 * @param local
	 * @throws IOException
	 */
	public void download(String remote, String local) throws IOException {
		fileSystem.copyToLocalFile(new Path(remote), new Path(local));
		System.out.println("Download from " + remote + " to " + local);
	}

	/**
	 * 这个是没法通过一条命令来实现的。 但是创建文件是有命令的：hadoop fs -touchz /chris/hehe 只不过里面的内容是空的。
	 * 
	 * @param file
	 * @param content
	 * @throws IOException
	 */
	public void createFile(String file, String content) throws IOException {
		byte[] buff = content.getBytes();
		FSDataOutputStream out = fileSystem.create(new Path(file));
		out.write(buff, 0, buff.length);
		out.close();
	}

	public static void main(String[] args) throws Exception {
		// 在实现这个config时，它会自动去加载resources下的这几个配置文件
		Configuration config = new Configuration();
		config.addResource(new Path("hdfs-site.xml"));
		config.addResource(new Path("core-site.xml"));
		System.out.println(config.get("fs.defaultFS"));
		HDFSClient client = new HDFSClient(config);
		// client.mkdir("/chris/client");

		// client.rmr("/chris/client");
		// client.ls("/chris");
		// client.cat("/chris/test");
		client.ls("/tmp/");
		client.copyFile("/core-site.xml", "/tmp/");
		client.ls("/tmp/");
		// client.download("/chris/test", "src/main/resources/");
		// client.createFile("/chris/client.txt", "ddddddddd");
		client.close();
	}
}
