import http.client
import json
import csv
import time
from urllib.parse import urlencode

#############################################################################################################################

class Graph:

    # Do not modify
    def __init__(self, with_nodes_file=None, with_edges_file=None):
        """
        option 1:  init as an empty graph and add nodes
        option 2: init by specifying a path to nodes & edges files
        """
        self.nodes = []                # list of (id:str, name:str)
        self.edges = []                # list of (source_id:str, target_id:str)
        self._node_ids = set()         # to ensure unique nodes
        self._edges_set = set()        # to ensure unique undirected edges (sorted tuple)

        if with_nodes_file and with_edges_file:
            nodes_CSV = csv.reader(open(with_nodes_file, encoding="utf-8"))
            nodes_CSV = list(nodes_CSV)[1:]
            self.nodes = [(n[0], n[1]) for n in nodes_CSV]
            self._node_ids = {n[0] for n in self.nodes}

            edges_CSV = csv.reader(open(with_edges_file, encoding="utf-8"))
            edges_CSV = list(edges_CSV)[1:]
            self.edges = [(e[0], e[1]) for e in edges_CSV]
            self._edges_set = {tuple(sorted((e[0], e[1]))) for e in self.edges}

    def add_node(self, id: str, name: str) -> None:
        """
        add a tuple (id, name) representing a node to self.nodes if it does not already exist
        The graph should not contain any duplicate nodes
        """
        sid = str(id)
        sname = str(name) if name is not None else ""
        sname = sname.replace(",", "")
        if sid not in self._node_ids:
            self.nodes.append((sid, sname))
            self._node_ids.add(sid)

    def add_edge(self, source: str, target: str) -> None:
        """
        Add an edge between two nodes if it does not already exist.
        An edge is represented by a tuple containing two strings: e.g.: ('source', 'target').
        Where 'source' is the id of the source node and 'target' is the id of the target node
        e.g., for two nodes with ids 'a' and 'b' respectively, add the tuple ('a', 'b') to self.edges
        """
        u = str(source); v = str(target)
        if u == v:
            return  # no self-loops
        a, b = (u, v) if u < v else (v, u)      # undirected canonical form
        if (a, b) not in self._edges_set:
            self._edges_set.add((a, b))
            self.edges.append((a, b))


    def total_nodes(self) -> int:
        """
        Returns an integer value for the total number of nodes in the graph
        """
        return len(self.nodes)

    def total_edges(self) -> int:
        """
        Returns an integer value for the total number of edges in the graph
        """
        return len(self.edges)

    def max_degree_nodes(self) -> dict:
        """
        Return the node(s) with the highest degree
        Return multiple nodes in the event of a tie
        Format is a dict where the key is the node_id and the value is an integer for the node degree
        e.g. {'a': 8}
        or {'a': 22, 'b': 22}
        """
        deg = {}
        for a, b in self.edges:
            deg[a] = deg.get(a, 0) + 1
            deg[b] = deg.get(b, 0) + 1
        if not deg:
            return {}
        mx = max(deg.values())
        return {nid: d for nid, d in deg.items() if d == mx}

    def print_nodes(self):
        """
        No further implementation required
        May be used for de-bugging if necessary
        """
        print(self.nodes)

    def print_edges(self):
        """
        No further implementation required
        May be used for de-bugging if necessary
        """
        print(self.edges)

    # Do not modify
    def write_edges_file(self, path="edges.csv")->None:
        """
        write all edges out as .csv
        :param path: string
        :return: None
        """
        edges_path = path
        edges_file = open(edges_path, 'w', encoding='utf-8')
        edges_file.write("source" + "," + "target" + "\n")
        for e in self.edges:
            edges_file.write(e[0] + "," + e[1] + "\n")
        edges_file.close()
        print("finished writing edges to csv")

    # Do not modify
    def write_nodes_file(self, path="nodes.csv")->None:
        """
        write all nodes out as .csv
        :param path: string
        :return: None
        """
        nodes_path = path
        nodes_file = open(nodes_path, 'w', encoding='utf-8')
        nodes_file.write("id,name" + "\n")
        for n in self.nodes:
            nodes_file.write(n[0] + "," + n[1] + "\n")
        nodes_file.close()
        print("finished writing nodes to csv")


class  TMDBAPIUtils:

    # Do not modify
    def __init__(self, api_key:str):
        self.api_key = api_key
        self._host = "api.themoviedb.org"
        self._base = "/3"

    def _get(self, path: str, params: dict) -> dict:
        """
        Minimal HTTP GET using http.client with retry/backoff.
        Returns parsed JSON dict or {} on failure.
        """
        params = dict(params or {})
        params.setdefault("language", "en-US")
        params.setdefault("api_key", self.api_key)

        qs = "?" + urlencode(params)
        full_path = f"{self._base}{path}{qs}"
        print(full_path)

        backoffs = [0.25, 0.5, 1.0]
        for i, delay in enumerate(backoffs):
            try:
                conn = http.client.HTTPSConnection(self._host, timeout=20)
                conn.request("GET", full_path)
                resp = conn.getresponse()
                data = resp.read()
                conn.close()
                if resp.status == 200 and data:
                    try:
                        return json.loads(data.decode("utf-8"))
                    except Exception:
                        return {}
            except Exception:
                # brief backoff
                time.sleep(delay)
        return {}

    def get_movie_cast(self, movie_id:str, limit:int=None, exclude_ids:list[int]=None) -> list:
        """
        Get the movie cast for a given movie id, with optional parameters to exclude a cast member
        from being returned and/or to limit the number of returned cast members
        documentation url: https://developers.themoviedb.org/3/movies/get-movie-credits

        :param string movie_id: a movie_id
        :param list exclude_ids: a list of ints containing ids of cast members that should be excluded from the returned result
        :param int limit: limit the number of results returned to this value (after sorting by 'order' asc)
        :return: list of dicts (the 'cast' array from the API), possibly filtered/limited
        """
        data = self._get(f"/movie/{movie_id}/credits", params={})
        cast = data.get("cast") or []
        filtered = []
        ex = set(str(x) for x in (exclude_ids or []))
        for c in cast:
            cid = str(c.get("id") or "")
            if not cid or cid in ex:
                continue
            ord_val = c.get("order", None)
            if isinstance(ord_val, int) and 0 <= ord_val < limit:
                filtered.append(c)

        filtered.sort(key=lambda c: c.get("order", 10**9))
        if limit is not None:
            filtered = filtered[:int(limit)]
        return filtered

    def get_movie_credits_for_person(self, person_id: str, start_date: str = None, end_date: str = None) -> list:
        """
        Using the TMDb API, get the movie credits for a person serving in a cast role
        documentation url: https://developers.themoviedb.org/3/people/get-person-movie-credits

        :param string person_id: the id of a person
        :param string start_date: optional filter: include only credits with a 'release_date' on or after start_date ('YYYY-MM-DD')
        :param string end_date: optional filter: include only credits with a 'release_date' on or before end_date ('YYYY-MM-DD')
        :return: list of dicts (the 'cast' array from the API) filtered by the optional dates (inclusive)
        """
        data = self._get(f"/person/{person_id}/movie_credits", params={})
        cast = data.get("cast") or []
        # Only include entries that have a release_date string; filter by window 
        out = []
        for credit in cast:
            rd = credit.get("release_date") or ""
            if not rd:
                continue
            if start_date and rd < start_date:
                continue
            if end_date and rd > end_date:
                continue
            out.append(credit)
        return out


def return_name()->str:
    """
    Return a string containing your GT Username
    e.g., gburdell3
    Do not return your 9 digit GTId
    """
    return "GTUsername"  # TODO



def build_coactor_graph_for_1999(api_key: str) -> Graph:
    tmdb = TMDBAPIUtils(api_key)
    g = Graph()
    SEED_ID = "2975"
    SEED_NAME = "Laurence Fishburne"
    YEAR_START = "1999-01-01"
    YEAR_END   = "1999-12-31"

    # Initialize with seed node
    g.add_node(SEED_ID, SEED_NAME)

    # --- BASE GRAPH ---
    base_new_nodes = []
    credits = tmdb.get_movie_credits_for_person(SEED_ID, YEAR_START, YEAR_END)
    for credit in credits:
        movie_id = str(credit.get("id"))
        if not movie_id:
            continue
        cast = tmdb.get_movie_cast(movie_id, limit=5, exclude_ids=[int(SEED_ID)])
        for c in cast:
            cid = str(c.get("id") or "")
            cname = c.get("name") or ""
            if not cid:
                continue
            # add node if new
            if cid not in g._node_ids:
                g.add_node(cid, cname)
                base_new_nodes.append(cid)
            # add edge between seed and this co-actor
            g.add_edge(SEED_ID, cid)

    # --- EXPANSION LOOPS (do 2 times) ---
    nodes_to_expand = base_new_nodes[:]  
    for _iter in range(2):
        new_nodes_this_iter = []
        # For each node in the current frontier
        for actor_id in list(nodes_to_expand):
            credits2 = tmdb.get_movie_credits_for_person(actor_id, YEAR_START, YEAR_END)
            for c2 in credits2:
                mid = str(c2.get("id") or "")
                if not mid:
                    continue
                cast2 = tmdb.get_movie_cast(mid, limit=5, exclude_ids=[int(actor_id)])
                for co in cast2:
                    coid = str(co.get("id") or "")
                    coname = co.get("name") or ""
                    if not coid:
                        continue
                    if coid not in g._node_ids:
                        g.add_node(coid, coname)
                        new_nodes_this_iter.append(coid)
                    g.add_edge(actor_id, coid)
        nodes_to_expand = new_nodes_this_iter  

    return g
  

#############################################################################################################################
#
# BUILDING YOUR GRAPH
#
# Working with the API:  See use of http.request: https://docs.python.org/3/library/http.client.html#examples
#
# Using TMDb's API, build a co-actor network for the actor's/actress' movies released in 1999.
# In this graph, each node represents an actor
# An edge between any two nodes indicates that the two actors/actresses acted in a movie together in 1999.
# i.e., they share a movie credit.
# e.g., An edge between Samuel L. Jackson and Robert Downey Jr. indicates that they have acted in one
# or more movies together in 1999.
#
# For this assignment, we are interested in a co-actor network of movies in 1999; specifically,
# we only want the first 5 co-actors in each movie credit with a release date in 1999.
# Build your co-actor graph on the actor 'Laurence Fishburne' w/ person_id 2975.
#
# You will need to add extra functions or code to accomplish this.  We will not directly call or explicitly grade your
# algorithm. We will instead measure the correctness of your output by evaluating the data in your nodes.csv and edges.csv files.
#
# GRAPH SIZE
# Since the TMDB API is a live database, the number of nodes / edges in the final graph will vary slightly depending on when
# you execute your graph building code. We take this into account by rebuilding the solution graph every few days and
# updating the auto-grader.  We compare your graph to our solution with a margin of +/- 200 for nodes and +/- 300 for edges.
# 
# e.g., if the current solution contains 507 nodes then the min/max range is 307-707.
# The same method is used to calculate the edges with the exception of using the aforementioned edge margin.
# ----------------------------------------------------------------------------------------------------------------------
# BEGIN BUILD CO-ACTOR NETWORK
#
# INITIALIZE GRAPH
#   Initialize a Graph object with a single node representing Laurence Fishburne
#
# BEGIN BUILD BASE GRAPH:
#   Find all of Laurence Fishburne's movie credits that have a release date in 1999.
#   FOR each movie credit:
#   |   get the movie cast members having an 'order' value between 0-4 (these are the co-actors)
#   |
#   |   FOR each movie cast member:
#   |   |   using graph.add_node(), add the movie cast member as a node (keep track of all new nodes added to the graph)
#   |   |   using graph.add_edge(), add an edge between the Laurence Fishburne (actor) node
#   |   |   and each new node (co-actor/co-actress)
#   |   END FOR
#   END FOR
# END BUILD BASE GRAPH
#
#
# BEGIN LOOP - DO 2 TIMES:
#   IF first iteration of loop:
#   |   nodes = The nodes added in the BUILD BASE GRAPH (this excludes the original node of Laurence Fishburne!)
#   ELSE
#   |    nodes = The nodes added in the previous iteration:
#   ENDIF
#
#   FOR each node in nodes:
#   |  get the movie credits for the actor that have a release date in 1999.
#   |
#   |   FOR each movie credit:
#   |   |   try to get the 5 movie cast members having an 'order' value between 0-4
#   |   |
#   |   |   FOR each movie cast member:
#   |   |   |   IF the node doesn't already exist:
#   |   |   |   |    add the node to the graph (track all new nodes added to the graph)
#   |   |   |   ENDIF
#   |   |   |
#   |   |   |   IF the edge does not exist:
#   |   |   |   |   add an edge between the node (actor) and the new node (co-actor/co-actress)
#   |   |   |   ENDIF
#   |   |   END FOR
#   |   END FOR
#   END FOR
# END LOOP
#
# Your graph should not have any duplicate edges or nodes
# Write out your finished graph as a nodes file and an edges file using:
#   graph.write_edges_file()
#   graph.write_nodes_file()
#
# END BUILD CO-ACTOR NETWORK
# ----------------------------------------------------------------------------------------------------------------------

# Exception handling and best practices
# - You should use the param 'language=en-US' in all API calls to avoid encoding issues when writing data to file.
# - If the actor name has a comma char ',' it should be removed to prevent extra columns from being inserted into the .csv file
# - Some movie_credits do not return cast data. Handle this situation by skipping these instances.
# - While The TMDb API does not have a rate-limiting scheme in place, consider that making hundreds / thousands of calls
#   can occasionally result in timeout errors. If you continue to experience 'ConnectionRefusedError : [Errno 61] Connection refused',
#   - wait a while and then try again.  It may be necessary to insert periodic sleeps when you are building your graph.


if __name__ == "__main__":
    # Optional manual run:
    # Set your TMDb v3 API key in the environment as TMDB_API_KEY, then run:
    #   python Q1.py
    # It will write nodes.csv and edges.csv in the current directory.
    api_key = None
    try:
        import os as _os
        api_key = _os.environ.get("TMDB_API_KEY")
    except Exception:
        api_key = None

    if not api_key:
        print("Set TMDB_API_KEY in your environment to build the graph, or import this module and call build_coactor_graph_for_1999(api_key).")
    else:
        graph = build_coactor_graph_for_1999(api_key)
        graph.write_nodes_file("nodes.csv")
        graph.write_edges_file("edges.csv")
        print("nodes:", graph.total_nodes(), "edges:", graph.total_edges())
        print("max-degree:", graph.max_degree_nodes())


