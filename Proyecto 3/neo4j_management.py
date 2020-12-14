#Librerias para neo4j
from py2neo import Graph
from py2neo import Node
from sys import stdin
import csv

#Conexion a local al neo4j
uri = "bolt://localhost:7687"
graph = Graph(uri, user = "neo4j", password = "bigdata")

def edge_parser():
	V = 1976
	E = 17235
	G = [[] for _ in range(V)]
	header = stdin.readline().split()
	for i in range(E):
	    line = stdin.readline().strip()
	    data = line.split(",")
	    u = int(data[0])
	    v = int(data[1])
	    G[u].append(v)
	return G

def create_nodes():
	V = 1976
	for u in range(V):
		new_node = Node("WikipediaPage", PageID=u)
		graph.create(new_node)

def create_edges():
	G = edge_parser()
	n = len(G)
	for u in range(n):
		for v in G[u]:
			graph.evaluate('''
			MATCH(p1:WikipediaPage) where p1.PageID = $u
			MATCH(p2:WikipediaPage) where p2.PageID = $v
			CREATE (p1)-[:Hyperlink]->(p2)
			''', parameters = {'u': u, 'v': v})

def graph_metrics():
	global graph
	#Creacion de los nodos en neo4j
    #create_nodes()
	#Creacion de los arcos en neo4j
    #create_edges()
	V = 1976
	#Calculo de grados
	G_Indegrees = []
	G_Outdegrees = []
	for u in range(V):
		ind = graph.evaluate('''
		match(p1:WikipediaPage) where p1.PageID= $u
		match(p2:WikipediaPage) where (p2) -[:Hyperlink]-> (p1) return count(p2)
		''', parameters = {'u': u})
		G_Indegrees.append(ind)
	for u in range(V):
		outd = graph.evaluate('''
		match(p1:WikipediaPage) where p1.PageID= $u
		match(p2:WikipediaPage) where (p1) -[:Hyperlink]-> (p2) return count(p1)
		''', parameters = {'u': u})
		G_Outdegrees.append(outd)

	#Calculo del numero de SCC
	query = graph.run('''
	CALL gds.alpha.scc.stream({nodeProjection: 'WikipediaPage', relationshipProjection: 'Hyperlink'})
	''').to_table()
	scc = dict()
	for i in range(len(query)):
		if query[i][1] in scc:
			scc[query[i][1]].append(query[i][0])
		else:
			scc[query[i][1]] = [query[i][0]]
	G_scc = [None for _ in range(V)]
	for component in scc:
		for x in scc[component]:
			G_scc[x] = component

	#Calculo de centralidad del vector propio
	G_betweenness = []
	query = graph.run('''
	CALL gds.betweenness.stream({nodeProjection: 'WikipediaPage', relationshipProjection: 'Hyperlink'})
	''').to_table()
	for i in range(len(query)):
		G_betweenness.append(query[i][1])

	#Calculo de coeficiente de cercania
	G_closeness = []
	query = graph.run('''
	CALL gds.alpha.closeness.stream({nodeProjection: 'WikipediaPage', relationshipProjection: 'Hyperlink'})
	''').to_table()
	for i in range(len(query)):
		G_closeness.append(query[i][1])

	return G_Indegrees, G_Outdegrees, G_scc, G_betweenness, G_closeness

m1, m2, m3, m4, m5 = graph_metrics()
fields = ["Indegree", "Outdegree", "NumSCC", "betweenness", "closeness"]
filename = "graph_metrics.csv"
rows = []
for i in range(1976):
	row = [m1[i], m2[i], m3[i], m4[i], m5[i]]
	rows.append(row)
with open(filename, 'w') as csvfile:
	csvwriter = csv.writer(csvfile)
	csvwriter.writerow(fields)
	csvwriter.writerows(rows)
