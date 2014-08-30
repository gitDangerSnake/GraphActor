package edu.hnu.cg.framework;

import java.nio.MappedByteBuffer;
import java.util.Arrays;

import kilim.Mailbox;
import kilim.Pausable;
import kilim.Scheduler;
import kilim.Task;

import edu.hnu.cg.graph.Graph;
import edu.hnu.cg.graph.Section;
import edu.hnu.cg.graph.datablocks.BytesToValueConverter;
import edu.hnu.cg.graph.datablocks.IntConverter;
import edu.hnu.cg.graph.userdefine.GraphConfig;

public class Worker<VertexValueType, EdgeValueType, MsgValueType> extends Task {

	private byte[] valueTemplate;

	private int id; // worker id
	private int k; // worker : vertices
	private byte[] record;

	private Manager<VertexValueType, EdgeValueType, MsgValueType> mgr;
	private Section currentSection;
	private int numSections;
	@SuppressWarnings("rawtypes")
	private Handler handler;
	private BytesToValueConverter<VertexValueType> vertexValueTypeBytesToValueConverter;
	private BytesToValueConverter<EdgeValueType> edgeValueTypeBytesToValueConverter;
	private MsgConverter<MsgValueType> msgConverter;

	// 顶点信息 record : length , vid , valueoffset len1-> vid , weight ->
	private int vid;
	private VertexValueType val;
	private int valueOffset;

	private int outDegree;
	private int[] outEdges;
	private EdgeValueType[] oevts;

	private int superstep = 0;
	private int sectionCounter;
	private byte[][] currentMsgbuffer;
	private byte[][] nextMsgbuffer;

	private Mailbox<Integer> mailbox = new Mailbox<>(10);

	public Worker(int id, Manager<VertexValueType, EdgeValueType, MsgValueType> mgr,
			Handler<VertexValueType, EdgeValueType, MsgValueType> handler,
			Combiner<MsgValueType> combiner, MsgConverter<MsgValueType> msgConverter,
			BytesToValueConverter<VertexValueType> vertexValueTypeBytesToValueConverter,
			BytesToValueConverter<EdgeValueType> edgeValueTypeBytesToValueConverter, int superstep) {

		this.id = id;
		this.mgr = mgr;
		this.handler = handler;
		this.combiner = combiner;
		this.msgConverter = msgConverter;
		this.vertexValueTypeBytesToValueConverter = vertexValueTypeBytesToValueConverter;
		this.edgeValueTypeBytesToValueConverter = edgeValueTypeBytesToValueConverter;
		if (vertexValueTypeBytesToValueConverter != null) {
			valueTemplate = new byte[vertexValueTypeBytesToValueConverter.sizeOf()];
		}

		this.superstep = superstep;
		scheduler = Scheduler.getDefaultScheduler();
		numSections = Graph.numSections;
		currentMsgbuffer = new byte[numSections][];
		nextMsgbuffer = new byte[numSections][];

	}

	private boolean running = true;
	
	private void updateValue(){
		MappedByteBuffer dataBuffer = currentSection.getVertexDataBuffer();
		synchronized(dataBuffer){
			dataBuffer.position(valueOffset);
			dataBuffer.put(valueTemplate, 0, valueTemplate.length);
			System.out.println(val);
			
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute() throws Pausable {

		while (running) {

			int signal = mailbox.get();

			if (signal == Signal.LOAD) {
				currentSection = mgr.getCurrentSection();
			}

			record = getRecord();
			if (record != null) {
				extractRecord(record); // 解析二进制数据
				// 如果消息不为空，处理消息

				if (currentMsgbuffer[sectionCounter] != null) {

					val = (VertexValueType) handler.compute(msgConverter.getFrom(currentMsgbuffer[sectionCounter]),
									msgConverter.getTo(currentMsgbuffer[sectionCounter]),
									val,
									msgConverter.getValue(currentMsgbuffer[sectionCounter]),
									superstep);
					
					currentMsgbuffer[sectionCounter] = null;

					// 更新顶点的value值
					vertexValueTypeBytesToValueConverter.setValue(valueTemplate, val);
					updateValue();
				}

				// 发送消息
				byte[] msg = null;
				for (int i = 0; i < outDegree; i++) {
//					System.out.println("outEdges's length is : " + outEdges.length
//							+ " oevets's length is: " + oevts.length + " outDegree is : "
//							+ outDegree)
					msg = handler.genMessage(vid, outEdges[i], val, (oevts.length != 0 ? oevts[i]
							: null), msgConverter);
					int workerId = location(outEdges[i]);
					mgr.getWorker(workerId).combineMessage(msg, outEdges[i] / Graph.sectionSize);
				}

			}
			sectionCounter++;
			mgr.putSignal(Signal.DONE);

			if (sectionCounter == numSections) {
				superstep++;
				sectionCounter = 0;
				swapBuffer();
			}

			clean();
		}

	}

	private void clean() {
		outEdges = null;
		oevts = null;
	}

	private Combiner<MsgValueType> combiner;

	private synchronized void combineMessage(byte[] msg, int id) {
		if (nextMsgbuffer[id] == null)
			nextMsgbuffer[id] = msg;
		else
			combiner.combine(nextMsgbuffer[id], msg, msgConverter);

	}

	public byte[] getRecord() {
		byte[] data = null;
		int offset = id * Graph.offsetWidth;
		int fetchIndex = 0;
		byte[] intVal = new byte[4];
		MappedByteBuffer vertexInforBuffer = currentSection.getVertexInformationBuffer();
		MappedByteBuffer vertexIndexBuffer = currentSection.getIndexBuffer();

		if (offset < vertexIndexBuffer.capacity()) {

			synchronized (vertexIndexBuffer) {
				vertexIndexBuffer.position(offset);
				vertexIndexBuffer.get(intVal, 0, 4);
				fetchIndex = byteArrayToInt(intVal);
			}

			synchronized (vertexInforBuffer) {
				vertexInforBuffer.position(fetchIndex);
				vertexInforBuffer.get(intVal, 0, 4);
				int len = byteArrayToInt(intVal);

				data = new byte[len];
				vertexInforBuffer.position(fetchIndex);

				vertexInforBuffer.get(data, 0, len);
			}
		}

		return data;

	}

	

	/**
	 * 解析二进制record
	 * 
	 * @param rd
	 *            : 二进制顶点邻接数据
	 * 
	 * */
	@SuppressWarnings("unchecked")
	private void extractRecord(byte[] rd) {

		int sizeof = edgeValueTypeBytesToValueConverter == null ? 0
				: edgeValueTypeBytesToValueConverter.sizeOf();
		IntConverter inc = new IntConverter();

		int len = rd.length;
		int i = 4;
		if (len > 12) {

			vid = readRecord(rd, i, 4, inc);
			i += 4;
			valueOffset = readRecord(rd, i, 4, inc);
			i += 4;
			val = getVertexValue(valueOffset);

			outDegree = readRecord(rd, i, 4, inc);
			i += 4;
			outEdges = new int[outDegree];
			if (sizeof == 0)
				oevts = (EdgeValueType[]) new Object[0];
			else
				oevts = (EdgeValueType[]) new Object[outDegree];

			int p = 0;
			int q = 0;

			while (q < outDegree) {
				readEdgeRecord(rd, i, sizeof, p, inc, outEdges, oevts);
				p++;
				i += (4 + sizeof);
				q++;
			}
		}

	}

	private void readEdgeRecord(byte[] rd, int pos, int len, int p, IntConverter converter,
			int[] d, EdgeValueType[] v) {
		byte[] tmp = new byte[4];
		byte[] edge = new byte[len];

		// 读取vid
		System.arraycopy(rd, pos, tmp, 0, 4);
		pos += 4;
		d[p] = converter.getValue(tmp);

		// 读取edge_value
		if (len != 0) {
			System.arraycopy(rd, pos, edge, 0, len);
			v[p] = edgeValueTypeBytesToValueConverter.getValue(edge);
		}

	}

	private <T> T readRecord(byte[] arr, int pos, int len, BytesToValueConverter<T> converter) {
		byte[] tmp = new byte[len];
		System.arraycopy(arr, pos, tmp, 0, len);
		T val = converter.getValue(tmp);
		return val;
	}

	private VertexValueType getVertexValue(int offset) {
		byte[] valueTemplate = new byte[vertexValueTypeBytesToValueConverter.sizeOf()];
		VertexValueType value;
		MappedByteBuffer DataBuffer = currentSection.getVertexDataBuffer();
		synchronized (DataBuffer) {
			DataBuffer.position(offset);
			DataBuffer.get(valueTemplate, 0, valueTemplate.length);
			value = vertexValueTypeBytesToValueConverter.getValue(valueTemplate);
		}
		return value;
	}

	private int location(int id) {
		return id % GraphConfig.sectionSize % GraphConfig.numWorkers;
	}

	public static byte[] intToByteArray(int val) {
		byte[] array = new byte[4];
		array[0] = (byte) ((val) & 0xff);
		array[1] = (byte) ((val >>> 8) & 0xff);
		array[2] = (byte) ((val >>> 16) & 0xff);
		array[3] = (byte) ((val >>> 24) & 0xff);
		return array;
	}

	public static int byteArrayToInt(byte[] array) {
		return ((array[3] & 0xff) << 24) + ((array[2] & 0xff) << 16) + ((array[1] & 0xff) << 8)
				+ (array[0] & 0xff);
	}

	private void swapBuffer() {
		byte[][] swap = currentMsgbuffer;
		currentMsgbuffer = nextMsgbuffer;
		nextMsgbuffer = swap;
	}

	public void putSignal(int signal) throws Pausable {
		mailbox.put(signal);

	}
}
