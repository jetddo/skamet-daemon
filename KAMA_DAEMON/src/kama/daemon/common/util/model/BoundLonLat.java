package kama.daemon.common.util.model;

public class BoundLonLat {
	
	private double top;
	private double bottom;
	private double left;
	private double right;
	
	public BoundLonLat(double top, double bottom, double left, double right) {
		super();
		this.top = top;
		this.bottom = bottom;
		this.left = left;
		this.right = right;
	}
	
	public double getTop() {
		return top;
	}
	public void setTop(double top) {
		this.top = top;
	}
	public double getBottom() {
		return bottom;
	}
	public void setBottom(double bottom) {
		this.bottom = bottom;
	}
	public double getLeft() {
		return left;
	}
	public void setLeft(double left) {
		this.left = left;
	}
	public double getRight() {
		return right;
	}
	public void setRight(double right) {
		this.right = right;
	}

	@Override
	public String toString() {
		return "BoundLonLat [top=" + top + ", bottom=" + bottom + ", left=" + left + ", right=" + right + "]";
	}
}