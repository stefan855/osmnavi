# Road Network Algorithms

## Project
Extract the road graph from OSM data dumps, implement various graph algorithms to preprocess the graph (for example [Tarjan](https://en.wikipedia.org/wiki/Bridge_(graph_theory)#Tarjan%27s_bridge-finding_algorithm) or [Louvain](https://en.wikipedia.org/wiki/Louvain_method)), and experiment with routing algorithms such as [Dijkstra](https://en.wikipedia.org/wiki/Dijkstra%27s_algorithm) or [A*](https://en.wikipedia.org/wiki/A*_search_algorithm).

## Status
Under Construction. Expect code to be buggy and unstable. Use at your own risk.

## Goals
* I do this for fun, and because I always wanted to program routing algorithms. These algorithms looked magic to me when I started to program many years ago.
* I want to utilize the 12 cores and 96GB Ram of my home computer, which normally just sits under my desk idling around.
* Develop efficient and simple routing algorithms that adapt to dynamic situations such as traffic jams or blocked roads. Because of this, it seems impossible (or too complicated) to use aggressive precomputation of routes as in [Contraction hierarchies](https://en.wikipedia.org/wiki/Contraction_hierarchies). 
* Routing should be efficient enough to run completely on smart phones. This helps protect the privacy of the user. Luckily, this becomes easier every year with improvements to the typical phone hardware. 

## Tasks done
1. Efficiently read OSM data dumps in pbf format with multiple threads ([read_osm_pbf.h](./src/osm/read_osm_pbf.h)).
2. Extract **country polygons** from OSM data dumps ([extract_admin.h](./src/bin/extract_admin.cpp)).
3. Given a point (lon,lat), efficiently find the country it belongs to ([tiled_country_lookup.h](./src/geometry/tiled_country_lookup.h)).
4. Support per-country defaults such as maxspeed and access. This uses a text config file, see [routing.cfg](./config/routing.cfg).
5. Find **dead end roads** and their associated bridges (see algorithm in [tarjan.h](./src/algos/tarjan.h)). Removing a bridge disconnects the graph at the bridge. The smaller of the two disconnected components is called dead end. Knowing dead ends is valuable, because they can be ignored for routing, unless the start or target nodes are in a dead end. See an example road network with some dead ends below (blue=road red=bridge green=dead end): ![Routing Network with Dead ends](./docs/pictures/example_deadend.png)
6. Implement standard routing algorithms. Currently, uni-directional versions of [Dijkstra](./src/algos/dijkstra.h) and [A*](./src/algos/astar.h) exist.
7. **Visualization** is an important aspect when working with graph algorithms. The [tile server](https://switch2osm.org/using-tiles/) (see [tile_server.cpp](./src/bin/tile_server.cpp)) adds overlay images to standard osm graphs in the browser, see [leaflet.html](./src/html/leaflet.html).
8. Basic Stuff:
   * Use multiple threads ([thread_pool.h](./src/base/thread_pool.h))
   * Command line parsing ([argli.h](./src/base/argli.h))
   * Huge, dynamic bitsets ([huge_bitset.h](./src/base/huge_bitset.h))
   * Memory pool ([simple_mem_pool.h](./src/base/simple_mem_pool.h))
   * Varbyte encoding of numbers, strings and lists of node ids ([varbyte.h](./src/base/varbyte.h))
   * Line clipping on viewports, using [Cohen-Sutherland algorithm](https://en.wikipedia.org/wiki/Cohen-Sutherland_algorithm), see [line_clipping.h](./src/geometry/line_clipping.h).
   * Distance between points on the earth, using the [haversine formula](https://en.wikipedia.org/wiki/Haversine_formula), see [distance.h](./src/geometry/distance.h).
1. Cluster the routing graph at the lowest level and for each cluster, precompute the cluster-internal routes between outside-connecting nodes. The user routing then runs on the much smaller graph induced by the clusters and the cluster-connecting edges. Precomputation of routes within clusters is done whenever needed and preferably on the user-device. To create good clusters, the [Louvain method](https://en.wikipedia.org/wiki/Louvain_method) is used, see  code in [louvain.h](./src/algos/louvain.h).
1. Support turn restrictions, including turn restrictions having multiple via-ways.
1. Support access=destination and other kinds of restrictions.
1. Support interactive routing in the browser.
1. Handle gates, bollards and similar obstacles.
1. Compute angles for edges. Needed for display in interactive routing, and for computing turn costs.
  
## Current Tasks
* Compute turn costs and use in routing.

## Tasks ahead
1. Assess the 'curviness' of ways and use it to lower maxspeed to real life values.
2. Store the routing graph in a file. Currently, the import of OSM data is re-done from scratch every time the routing graph is needed (see [build_graph_main.cpp](./src/bin/build_graph_main.cpp)).
1. Support more transportation means, especially bicycles and pedestrians. So far, development mainly targets cars.
1. Add routing configs for more countries (see [routing.cfg](config/routing.cfg)).
1. Support routing conditions from users, for instance "avoid toll roads", "stay withing country borders" or "only paved or better ways".
1. Support dynamic data such as traffic jams. This is similar to the previous point, since both require recomputation of travel times within clusters.
1. Support lanes. It isn't currently clear to me if lanes are needed for routing, or if they are only useful for the user experience during navigation.
1. Experiment with SIMD parallelization primitives available on modern processors, especially for cluster node travel time computation.

## Installation hints
1. Code is developed on a 64-bit PC (AMD64) using Ubuntu Linux. I haven't tried compiling or running it on any other operating system or platform.
2. Recently, I installed the repo on a clean install on kubuntu 25.4. Here are my notes:
```
sudo apt-get update && sudo apt-get install build-essential
sudo apt-get install protobuf-compiler libprotobuf-dev
sudo apt install libgd-dev
sudo apt install libosmpbf-dev
sudo apt install nlohmann-json3-dev

cd ~
mkdir src; cd src
mkdir osm; cd osm

# Download version 3.2.12 from https://github.com/perliedman/leaflet-routing-machine/releases 
# and unpack it in the osm directory:
tar xf ~/Downloads/leaflet-routing-machine-3.2.12.tar.gz

git clone https://github.com/stefan855/osmnavi
cd osmnavi
git submodule update --init --recursive
mkdir release
cd release/
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j 16

mkdir ../../data
# Download a planet dump and a smaller dump (for instance Switzerland) and store them in ../../data
# Webpage for the planet dumps: https://planet.openstreetmap.org/
#   Swiss mirror: https://mirror.init7.net/openstreetmap/pbf/
# Webpage for a country file: https://download.geofabrik.de
# Example: Downloaded files:
#   ~/src/osm/data/planet-latest.osm.pbf
#   ~/src/osm/data/switzerland-latest.osm.pbf

# Extract country borders, run the following in the release directory:
./extract_admin ../../data/planet-latest.osm.pbf /tmp/admin
# If no errors occurr (or if you can ignore the countries that have errors), then move the output directory as shown below.
mv /tmp/admin ../../data/admin
# Otherwise you might try another planet dump (some planet files have errors in the country boundaries such as incomplete polygons).

# Create visualisation data in /tmp. This takes about 25 seconds on my home computer.
./build_graph_main ../../data/switzerland-latest.osm.pbf

# Run tile server for visualization data
./tile_server
# Run routing server to answer interactive queries
./routing_server ../../data/switzerland-latest.osm.pbf

# Browse information in both servers.
file://<path_to_repo>/osmnavi/src/html/leaflet.html
```
