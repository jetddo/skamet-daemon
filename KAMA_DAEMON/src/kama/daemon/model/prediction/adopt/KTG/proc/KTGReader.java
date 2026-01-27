package kama.daemon.model.prediction.adopt.KTG.proc;

import java.io.File;
import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import kama.daemon.model.prediction.adopt.KTG.data.KTGLayer;
import ucar.nc2.NetcdfFile;

public final class KTGReader {

	// KTG 데이터 X, Y 그리드 갯수
	private static final int XDIM = 490;
	private static final int YDIM = 418;

	// KTG 고도(단위 : ft)
	private final float[] altitude;

	// KTG 고도별 레이어
	private final List<KTGLayer> layers = new ArrayList<>();

	public KTGReader(final String filePath) throws Exception {
		final NetcdfFile ncFile = NetcdfFile.open(new File(filePath).getAbsolutePath());

		// 고도를 읽어들인다.
		this.altitude = (float[]) ncFile.findVariable("lev_ft").read().getStorage();

		// KTG 값을 읽어들인다.
		final float[] KTG_AUC_20 = (float[]) ncFile.findVariable("KTG_AUC_20").read().getStorage();

		for (int index = 0; index < KTG_AUC_20.length; index += (XDIM * YDIM)) {
			this.layers.add(KTGLayer.fromBinary(XDIM, YDIM, Arrays.copyOfRange(KTG_AUC_20, index, index + (XDIM * YDIM))));
		}

		ncFile.close();
	}

	public KTGLayer getLayer(final int layerIndex) {
		if (layerIndex >= 0 && layerIndex < this.layers.size()) {
			return this.layers.get(layerIndex);
		}

		return null;
	}

	public KTGLayer[] getAllLayers() {
		return this.layers.toArray(new KTGLayer[this.layers.size()]);
	}

	public int layerCount() {
		return this.layers.size();
	}

	public float getAltitude(final int index) {
		if (index >= 0 && index < this.altitude.length) {
			return this.altitude[index];
		}

		return -1;
	}

	public void regridingAllLayers(final FloatBuffer latitudeBuffer, final FloatBuffer longitudeBuffer) {
		for (int index = 0; index < this.layers.size(); ++index) {
			final KTGLayer layer = this.layers.get(index);
			this.layers.set(index, layer.performRegriding(latitudeBuffer, longitudeBuffer));
		}
	}

}
