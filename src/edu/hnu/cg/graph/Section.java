package edu.hnu.cg.graph;

//import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.MappedByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;

public class Section {

	private int id;

	private String graphFilename;
	private String sectionFilename;
	private String vertexDataFilename;
	private String fetchIndexFilename;

	private long valueSize;

	private RandomAccessFile sectionFile;
	private MappedByteBuffer vertexInformationBuffer;
	private FileChannel vertexInfoFileChannel;

	private RandomAccessFile vertexDataFile;
	private MappedByteBuffer vertexDataBuffer;
	private FileChannel vertexDataFileChannel;

	private RandomAccessFile fetchIndexFile;
	private MappedByteBuffer indexBuffer;
	private FileChannel indexChannel;

	private volatile boolean loaded = false;
	private volatile boolean unloaded = false;

	public Section(int _id, String graphFilename, long vsize ) throws IOException {
		id = _id;
		this.graphFilename = graphFilename;
		valueSize = vsize;
	}

	// if this method is called , null pointer needed to be checked
	public MappedByteBuffer getVertexInformationBuffer() {
		return vertexInformationBuffer;
	}

	public MappedByteBuffer getVertexDataBuffer() {
		return vertexDataBuffer;
	}


	public MappedByteBuffer getIndexBuffer() {
		return indexBuffer;
	}

	public void load() throws IOException {

		sectionFilename = Filename.getSectionFilename(graphFilename, id);
		sectionFile = new RandomAccessFile(sectionFilename, "r");
		// 载入section信息文件
		if (sectionFile != null) {
			vertexInfoFileChannel = sectionFile.getChannel();
			vertexInformationBuffer = vertexInfoFileChannel.map(MapMode.READ_ONLY, 0, sectionFile.length());
			vertexInformationBuffer.position(0);
		} else {
			return;
		}

		fetchIndexFilename = Filename.getSectionFetchIndexFilename(graphFilename, id);
		fetchIndexFile = new RandomAccessFile(fetchIndexFilename, "r");
		if (fetchIndexFile != null) {
			indexChannel = fetchIndexFile.getChannel();
			indexBuffer = indexChannel.map(MapMode.READ_ONLY, 0, fetchIndexFile.length());
			indexBuffer.position(0);
		} else {
			return;
		}
		vertexDataFilename = Filename.getSectionVertexDataFilename(graphFilename, id);
		vertexDataFile = new RandomAccessFile(vertexDataFilename, "rw");
		// 载入顶点value数据文件
		if (vertexDataFile != null) {
			if (vertexDataFile.length() == 0) {
				vertexDataFileChannel = vertexDataFile.getChannel();
				vertexDataBuffer = vertexDataFileChannel.map(MapMode.READ_WRITE, 0, valueSize);
				vertexDataBuffer.position(0);
			} else {
				vertexDataFileChannel = vertexDataFile.getChannel();
				vertexDataBuffer = vertexDataFileChannel.map(MapMode.READ_WRITE, 0, vertexDataFile.length());
				vertexDataBuffer.position(0);
			}

		} else {
			return;
		}

		

		loaded = true;
		sectionFile.close();
		vertexDataFile.close();
		fetchIndexFile.close();

		vertexInfoFileChannel.close();
		vertexDataFileChannel.close();
		indexChannel.close();

	}

	public boolean isLoaded() {
		return loaded;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void clean(final MappedByteBuffer buffer) {
		AccessController.doPrivileged(new PrivilegedAction() {
			@Override
			public Object run() {
				try {
					Method getCleanerMethod = buffer.getClass().getMethod("cleaner", new Class[0]);
					getCleanerMethod.setAccessible(true);
					sun.misc.Cleaner cleaner = (sun.misc.Cleaner) getCleanerMethod.invoke(buffer, new Object[0]);
					cleaner.clean();
				} catch (Exception e) {
					e.printStackTrace();
				}
				return null;
			}
		});
	}

	public void unload() throws IOException {

		// 关闭之前需要将内容更新到磁盘
		vertexDataBuffer.force();

		clean(vertexInformationBuffer);
		clean(vertexDataBuffer);
		clean(indexBuffer);

		unloaded = true;
		loaded = false;

	}

	public boolean isUnloaded() {
		return unloaded;
	}

	public static void main(String[] args) throws IOException {
		Section section = new Section(0, "/home/doro/CG/google", 473524992);
		section.load();
		System.out.println(section.indexBuffer.capacity());
		for (int k = 0; k < 100; k++) {
			section.indexBuffer.get(0);
			System.out.println(section.indexBuffer.position());
		}

		int i = 0;
		while (section.vertexInformationBuffer.remaining() > 0) {
			int len = section.vertexInformationBuffer.getInt();
			System.out.print("Recorde " + i + " : " + len + ",");
			int vid = section.vertexInformationBuffer.getInt();
			System.out.print(vid + ",");
			long valueOffset = section.vertexInformationBuffer.getInt();
			System.out.print(valueOffset);
			int k = 0;
			while (k < (len - 12) / 8) {
				int d_id = section.vertexInformationBuffer.getInt();
				System.out.print("->" + d_id);
				long offset = section.vertexInformationBuffer.getInt();
				System.out.print("," + offset);
				k++;
			}
			System.out.println();
			i++;

		}

		section.unload();
	}
	
	
}
