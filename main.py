from leafs import *
from topics import *    
import os 
import datetime

def collect_nodes_with_topics(node, nodes_with_topics):
    if node.has_topics:
        nodes_with_topics.append(node)
    for child in node.children:
        collect_nodes_with_topics(child, nodes_with_topics)
        

def count_nodes(node):
    count = 1  # Count the current node
    for child in node.children:
        count += count_nodes(child)  # Recursively count child nodes
    return count

def main():
    # Load the tree
    with open("forum_tree.json", "r", encoding="utf-8") as f:
        loaded_tree_data = json.load(f)
        loaded_tree = ForumNode.from_dict(loaded_tree_data)

    # Collect nodes with topics
    nodes_with_topics = []
    collect_nodes_with_topics(loaded_tree, nodes_with_topics)

    # print("Nodes with topics to scrape:")
    # for node in nodes_with_topics:
        # print(f"- {node.title} ({node.url})")
        
    # Count and print the total number of nodes in the tree
    total_nodes = count_nodes(loaded_tree)
    print('The tree has', total_nodes, 'nodes in total')

    print('We have' , len(nodes_with_topics), 'nodes with topics to scrape')

    # File to store already scraped nodes
    SCRAPED_NODES_FILE = "scraped_nodes.json"

    # Load already scraped nodes from file
    def load_scraped_nodes():
        if os.path.exists(SCRAPED_NODES_FILE):
            with open(SCRAPED_NODES_FILE, "r") as file:
                return json.load(file)
        return {}  # Return empty dict if file doesn't exist

    # Save scraped nodes to file
    def save_scraped_nodes(scraped_nodes):
        with open(SCRAPED_NODES_FILE, "w") as file:
            json.dump(scraped_nodes, file, indent=4)

    # Load already scraped nodes
    scraped_nodes = load_scraped_nodes()

    from tqdm import tqdm
    len_posts = 0
    for node in tqdm(nodes_with_topics):
        # Check if node was already scraped (using title as unique identifier)
        if node.title not in scraped_nodes:
            os.makedirs(f'Data/{node.title}', exist_ok=True)
            len_posts += scrape_topics(node.url, node.title)
            
            # Mark node as scraped
            scraped_nodes[node.title] = {
                "url": node.url,
                "scraped_at": str(datetime.datetime.now())  # Add timestamp
            }
            
            # Save updated scraped nodes
            save_scraped_nodes(scraped_nodes)
            print(f"Scraped: {node.title}")
        else:
            print(f"Skipping already scraped: {node.title}")
            
    print('Total posts scraped:', len_posts)


if __name__ == "__main__":
    main()