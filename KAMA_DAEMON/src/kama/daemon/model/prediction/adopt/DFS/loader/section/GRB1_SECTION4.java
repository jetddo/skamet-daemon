package kama.daemon.model.prediction.adopt.DFS.loader.section;

public final class GRB1_SECTION4 {

	public int length;
	public int flag;
	public int unused_bits;
	public int Scale_E;
	public float minimum;
	public int num_bits;
	public int option;

	public void clear() {
		this.length = 0;
		this.flag = 0;
		this.unused_bits = 0;
		this.Scale_E = 0;
		this.minimum = 0.0f;
		this.num_bits = 0;
		this.option = 0;
	}

}
