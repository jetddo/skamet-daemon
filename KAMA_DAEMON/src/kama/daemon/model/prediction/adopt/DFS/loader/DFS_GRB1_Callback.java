package kama.daemon.model.prediction.adopt.DFS.loader;

import kama.daemon.model.prediction.adopt.DFS.loader.section.DFS_GRB1_INF;

public interface DFS_GRB1_Callback {

	boolean callback(final DFS_GRB1_INF inf, final float[][] dfsData);

}
