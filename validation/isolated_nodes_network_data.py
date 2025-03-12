import networkx as nx
import logging
from src.data_processing.network_data import get_network_data  # adjust import path as needed

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Retrieve the processed network data
data = get_network_data()
circuit_data_filtered = data['circuit_data_filtered']
transformer_data_filtered = data['transformer_data_filtered']
reactive_data_filtered = data['reactive_data_filtered']
all_nodes_df = data['all_nodes_df']

# Extract nodes list from all_nodes_df
nodes = set(all_nodes_df['Node'].dropna())

def extract_branches_and_nodes(circuit_data, transformer_data):
    branches = []
    all_extracted_nodes = set()
    try:
        if "Node 1" in circuit_data.columns and "Node 2" in circuit_data.columns:
            circuit_branches = circuit_data[["Node 1", "Node 2"]].dropna().values
            branches.extend([tuple(branch) for branch in circuit_branches])
            all_extracted_nodes.update(circuit_data["Node 1"].dropna().unique())
            all_extracted_nodes.update(circuit_data["Node 2"].dropna().unique())
        if "Node 1" in transformer_data.columns and "Node 2" in transformer_data.columns:
            transformer_branches = transformer_data[["Node 1", "Node 2"]].dropna().values
            branches.extend([tuple(branch) for branch in transformer_branches])
            all_extracted_nodes.update(transformer_data["Node 1"].dropna().unique())
            all_extracted_nodes.update(transformer_data["Node 2"].dropna().unique())
        logger.info(f"Extracted {len(branches)} branches and {len(all_extracted_nodes)} nodes successfully.")
    except Exception as e:
        logger.error(f"Error during branch and node extraction: {e}")
        raise
    return branches, all_extracted_nodes

branches, extracted_nodes = extract_branches_and_nodes(circuit_data_filtered, transformer_data_filtered)
missing_nodes = extracted_nodes - nodes
if missing_nodes:
    logger.warning(f"Missing nodes detected! The following nodes are in circuit/transformer data but not in the node list: {missing_nodes}")
    print("Missing nodes:", missing_nodes)
else:
    logger.info("All nodes in the circuit and transformer data are accounted for in the node data.")

G = nx.Graph()
G.add_nodes_from(nodes)
G.add_edges_from(branches)
isolated_nodes = list(nx.isolates(G))

def analyse_isolated_nodes(isolated_nodes, circuit_data, transformer_data, reactive_data, graph):
    details = []
    for node in isolated_nodes:
        node_details = {"Node": node}
        circuit_presence = (("Node 1" in circuit_data.columns and (circuit_data["Node 1"] == node).any()) or
                            ("Node 2" in circuit_data.columns and (circuit_data["Node 2"] == node).any()))
        transformer_presence = (("Node 1" in transformer_data.columns and (transformer_data["Node 1"] == node).any()) or
                                ("Node 2" in transformer_data.columns and (transformer_data["Node 2"] == node).any()))
        reactive_presence = (("Node" in reactive_data.columns) and (reactive_data["Node"] == node).any())
        node_details["In Circuit Data"] = circuit_presence
        node_details["In Transformer Data"] = transformer_presence
        node_details["In Reactive Data"] = reactive_presence
        node_degree = graph.degree(node)
        node_details["Degree"] = node_degree
        if not circuit_presence and not transformer_presence and not reactive_presence:
            isolation_cause = "Node not found in any circuit or transformer or reactive data."
        elif node_degree == 0:
            isolation_cause = "Node has no connected branches in the network."
        else:
            isolation_cause = "Node appears in input data but may be disconnected from the main network."
        node_details["Isolation Cause"] = isolation_cause
        details.append(node_details)
    return details

isolated_node_details = analyse_isolated_nodes(isolated_nodes, circuit_data_filtered, transformer_data_filtered, reactive_data_filtered, G)

# Print detailed analysis for each isolated node
for detail in isolated_node_details:
    print(f"Node: {detail['Node']}")
    print(f"  - In Circuit Data: {detail['In Circuit Data']}")
    print(f"  - In Transformer Data: {detail['In Transformer Data']}")
    print(f"  - In Reactive Data: {detail['In Reactive Data']}")
    print(f"  - Degree in Graph: {detail['Degree']}")
    print(f"  - Isolation Cause: {detail['Isolation Cause']}")
    print("\n")

# Count the isolated nodes per category
circuit_count = sum(1 for detail in isolated_node_details if detail["In Circuit Data"])
transformer_count = sum(1 for detail in isolated_node_details if detail["In Transformer Data"])
reactive_count = sum(1 for detail in isolated_node_details if detail["In Reactive Data"])

print(f"Number of isolated nodes: {len(isolated_nodes)}")
print(f"Isolated nodes present in Circuit Data: {circuit_count}")
print(f"Isolated nodes present in Transformer Data: {transformer_count}")
print(f"Isolated nodes present in Reactive Data: {reactive_count}")

logger.info("Isolated nodes analysis completed.")
