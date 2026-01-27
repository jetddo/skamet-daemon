package kama.daemon.model.prediction.adopt.KTG.data;

import org.apache.commons.lang3.ArrayUtils;

public class Layer2D implements Cloneable {

	protected int sizeX;
	protected int sizeY;
	protected float[] rawData;

	private Layer2D() {

	}

	protected Layer2D(final int width, final int height) {
		this.sizeX = width;
		this.sizeY = height;
		this.rawData = new float[width * height];
	}
	
	protected Layer2D(final int width, final int height, final float[] rawData) {
		this.sizeX = width;
		this.sizeY = height;
		this.rawData = rawData;
	}

	protected Layer2D(final Layer2D layer2D) {
		this.sizeX = layer2D.sizeX;
		this.sizeY = layer2D.sizeY;
		this.rawData = layer2D.rawData.clone();
	}

	public void setValue(final int x, final int y, final float value) {
		this.rawData[x + (y * this.sizeX)] = value;
	}

	public float getValue(final int x, final int y) {
		return this.rawData[x + (y * this.sizeX)];
	}

	public int getSizeX() {
		return this.sizeX;
	}

	public int getSizeY() {
		return this.sizeY;
	}

	public void flipVertical() {
		for (int x = 0; x < this.sizeX; ++x) {
			final float[] buffer = getVertical(x);

			ArrayUtils.reverse(buffer);
			setVerticalValues(x, buffer);
		}
	}

	public float[] getVertical(final int x) {
		final float[] buffer = new float[this.sizeY];

		for (int y = 0; y < this.sizeY; ++y) {
			buffer[y] = getValue(x, y);
		}

		return buffer;
	}

	public void setVerticalValues(final int x, final float[] buffer) {
		final int minY = buffer.length > this.sizeY ? this.sizeY : buffer.length;

		for (int y = 0; y < minY; ++y) {
			setValue(x, y, buffer[y]);
		}
	}

	public void setHorizontalValues(final int y, final float[] buffer) {
		final int minX = buffer.length > this.sizeX ? this.sizeX : buffer.length;

		for (int x = 0; x < minX; ++x) {
			setValue(x, y, buffer[y]);
		}
	}

	@Override
	public Layer2D clone() {
		Layer2D layer2D = new Layer2D();

		layer2D.sizeX = this.sizeX;
		layer2D.sizeY = this.sizeY;
		layer2D.rawData = this.rawData.clone();

		return layer2D;
	}

}
