package edu.hnu.cg.test;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class ObjectSharing extends Thread {
	int id;
	Test t;

	public ObjectSharing(int id, Test t) {
		this.id = id;
		this.t = t;
	}

	public void run() {
		if (id == 1)
			t.say1();
		else
			t.say2();
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
		return ((array[3] & 0xff) << 24) + ((array[2] & 0xff) << 16) + ((array[1] & 0xff) << 8) + (array[0] & 0xff);
	}
	public static void main(String[] args) {
		Test t = new Test();
		ObjectSharing s1 = new ObjectSharing(1, t);
		ObjectSharing s2 = new ObjectSharing(2, t);

		byte[][] bb =new byte[8][10];
		System.out.println(bb[1] == null);
		change(bb[1]);
		
		byte[] msg = bb[1];
		msg = null;
		System.out.println(bb[1]);
		
		ByteBuffer bbb = ByteBuffer.allocate(1024);
		byte[] p = intToByteArray(10);
		bbb.put(p);
		bbb.flip();
		byte[] ten = new byte[4];
		bbb.get(ten, 0, 4);
		System.out.println(Arrays.toString(bbb.array()));
		System.out.println(byteArrayToInt(ten));
		
		

	}
	
	public static void change(final byte[] arr){
	}

}

class Test {
	public void say1() {
			while (true) {
				System.out.println("11111111111111");
			}
	}

	public void say2() {
		while (true) {
			System.out.println("222222222222222");
		}
	}
}
