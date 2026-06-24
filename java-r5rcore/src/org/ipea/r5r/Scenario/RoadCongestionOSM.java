package org.ipea.r5r.Scenario;


import com.conveyal.r5.analyst.scenario.Modification;
import com.conveyal.r5.streets.EdgeStore;
import com.conveyal.r5.transit.TransportNetwork;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

import static com.conveyal.r5.streets.EdgeStore.EdgeFlag;

/**
 * Direction-aware road congestion by OSM id.
 *
 * R5 stores each OSM way as a pair of directed edges: even edge index = forward,
 * odd = backward (see EdgeStore: "All even numbered edges are forward").
 * This modification accepts SEPARATE forward/backward speed maps so that the
 * up (forward) and down (backward / reverse) probe speeds can be applied to the
 * correct directed edge. Pass the same value in both maps for undirected speeds.
 */
public class RoadCongestionOSM extends Modification {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(RoadCongestionOSM.class);

    /** The default value by which to scale when an osm id is not in the map. */
    public float defaultScaling = 1;

    /** HashMap key=[osm_id] value=[max_speed] for FORWARD (even) edges. */
    public HashMap<Long, Float> speedMapFwd;
    /** HashMap key=[osm_id] value=[max_speed] for BACKWARD (odd) edges. */
    public HashMap<Long, Float> speedMapBwd;

    public boolean absoluteMode = false;


    @Override
    public boolean resolve(TransportNetwork network) {
        TLongHashSet osmIdSet = new TLongHashSet();
        for (int i = 0; i < network.streetLayer.edgeStore.osmids.size(); i++) {
            osmIdSet.add(network.streetLayer.edgeStore.osmids.get(i));
        }
        TLongArrayList badIds = new TLongArrayList();
        // check both maps
        for (Long osmId : speedMapFwd.keySet()) {
            if (!osmIdSet.contains(osmId)) badIds.add(osmId);
        }
        for (Long osmId : speedMapBwd.keySet()) {
            if (!osmIdSet.contains(osmId) && !speedMapFwd.containsKey(osmId)) badIds.add(osmId);
        }
        if (!badIds.isEmpty()) {
            LOG.warn("Cannot find the following OSM IDs in network: {}", badIds);
        }
        return hasErrors();
    }

    @Override
    public boolean apply(TransportNetwork network) {
        LOG.info("Applying directional road congestion by OSM id...");

        EdgeStore edgeStore = network.streetLayer.edgeStore;
        EdgeStore.Edge edge = edgeStore.getCursor();
        network.streetLayer.edgeStore.flags = new TIntArrayList(network.streetLayer.edgeStore.flags);

        while (edge.advance()) {
            // pick the forward or backward map based on edge direction
            HashMap<Long, Float> map = edge.isForward() ? speedMapFwd : speedMapBwd;
            Float value = map.get(edge.getOSMID());

            float scaling = (value == null) ? defaultScaling : value;

            if (scaling == 0) {
                edge.clearFlag(EdgeFlag.ALLOWS_CAR);
            } else if (value != null && absoluteMode) {
                edge.setSpeedKph(value);
            } else {
                edge.setSpeed((short) (edge.getSpeed() * scaling));
            }
        }
        return hasErrors();
    }

    @Override
    public int getSortOrder() { return 95; }

    @Override
    public boolean affectsStreetLayer() { return true; }

    @Override
    public boolean affectsTransitLayer() { return false; }
}
