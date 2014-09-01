package edu.hnu.cg.framework;

import java.io.IOException;

import kilim.Mailbox;
import kilim.Pausable;
import kilim.Task;
import edu.hnu.cg.graph.Graph;
import edu.hnu.cg.graph.Section;
import edu.hnu.cg.graph.datablocks.BytesToValueConverter;
import edu.hnu.cg.graph.userdefine.GraphConfig;

public class Manager<VertexValueType , EdgeValueType , MsgValueType> extends Task {

	private Worker<VertexValueType, EdgeValueType, MsgValueType>[] workers;
	private Section[] sections;

	private int superstep = 0;
	private int currentSection;

	private Handler<VertexValueType, EdgeValueType, MsgValueType> handler;
	private Combiner<MsgValueType> combiner;
	private MsgConverter<MsgValueType> msgConverter;
	private BytesToValueConverter<MsgValueType> msgValueTypeBytesToValueConverter;
	private BytesToValueConverter<VertexValueType> vertexValueTypeBytesToValueConverter;
	private BytesToValueConverter<EdgeValueType> edgeValueTypeBytesToValueConverter;

	private Mailbox<Integer> mailbox = new Mailbox<>();

	public Section getCurrentSection() {
		return sections[currentSection];
	}

	public Manager(Graph<VertexValueType, EdgeValueType, MsgValueType> graph,Handler<VertexValueType, EdgeValueType, MsgValueType> handler,
			Combiner<MsgValueType> combiner,			
			BytesToValueConverter<MsgValueType> msgValueTypeBytesToValueConverter,
			BytesToValueConverter<VertexValueType> vertexValueTypeBytesToValueConverter,
			BytesToValueConverter<EdgeValueType> edgeValueTypeBytesToValueConverter) {
		sections = graph.sections;
		this.handler = handler;
		this.combiner = combiner;
		this.msgValueTypeBytesToValueConverter = msgValueTypeBytesToValueConverter;
		this.vertexValueTypeBytesToValueConverter = vertexValueTypeBytesToValueConverter;
		this.edgeValueTypeBytesToValueConverter = edgeValueTypeBytesToValueConverter;
		msgConverter = new MsgConverter<>(msgValueTypeBytesToValueConverter);
		
		initWorkers();
	}

	// 初始化workers
	@SuppressWarnings("unchecked")
	public void initWorkers() {
		int numWorkers = GraphConfig.numWorkers;
		workers = new Worker[numWorkers];

		for (int i = 0; i < numWorkers; i++) {
			// constructor should implements
			workers[i] = new Worker<>(i, this, handler, combiner, msgConverter,
					vertexValueTypeBytesToValueConverter, edgeValueTypeBytesToValueConverter,superstep);
		}

	}

	public Worker<VertexValueType, EdgeValueType, MsgValueType> getWorker(int i) {
		return workers[i];
	}

	private boolean running = true;

	@Override
	public void execute() throws Pausable {

		System.out.println("start workers");
		startWorkers();
		System.out.println("....start finished...");
		int counter = 0;

		while (running) {
			// 载入分区
			System.out.println("load section " + counter);
			load(counter);
			System.out.println("section " + counter + " loaded");
			
			// 发送消息,告知worker开始工作
			System.out.println("ask worker to work");
			for (Worker<VertexValueType, EdgeValueType, MsgValueType> w : workers) {
				w.putSignal(Signal.LOAD);
			}
			System.out.println("start to wait for section finish");

			// 等待分区完成
			while (workCounter != workers.length) {
				int signal = mailbox.get();
				// workers[signal].putSignal(signal);
				if (signal == Signal.DONE)
					workCounter++;

			}

			workCounter = 0;

			counter++;
			if (counter == sections.length) {
				counter = 0;
				superstep++;
			}

		}

	}

	private int workCounter = 0;

	private void load(int counter) {
		try {
			System.out.println(sections[counter] == null);
			sections[counter].load();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void unload(int counter) {
		try {
			sections[counter].unload();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void startWorkers() {

		for (Worker<VertexValueType, EdgeValueType, MsgValueType> t : workers) {
			t.start();
		}
	}

	public void putSignal(int s) throws Pausable {

		mailbox.put(s);
	}

}
