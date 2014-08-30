package edu.hnu.cg.framework;


public interface Combiner<MsgValueType> {

	public void combine( final byte[] combine, byte[] msg,MsgConverter<MsgValueType> converter);
}
