[out:json];
// The public street network
way["highway"~"^(trunk|primary|secondary|tertiary|unclassified|residential)$"](42.6290946,21.1066198,42.6969650,21.2136267)->.streets;

// Get nodes that connect between three or more street segments
node(way_link.streets:3-)->.connections;

// Get intersections between distinct streets
foreach .connections->.connection(
  // Get adjacent streets
  way(bn.connection);
  // If the names don't all match, add the node to the set of intersections
  if (u(t["name"]) == "< multiple values found >") {
    (.connection; .intersections;)->.intersections;
  }
);

.intersections out geom;
