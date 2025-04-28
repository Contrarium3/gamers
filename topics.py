from urllib.parse import urljoin, urlencode
from loggers import * 
from leafs import fetch_soup
from bs4 import BeautifulSoup, NavigableString
import json
import os 
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import unicodedata

def scrape_topics(url, title, max_workers=30):
    """
    Scrape all topics from a forum with parallel processing for individual topics
    
    Args:
        url: URL of the forum
        title: Title of the forum
        max_workers: Maximum number of parallel workers for topic scraping
    
    Returns:
        List of topic data
    """
    # GET ALL TOPICS, url is a link that has topics
    log_info(f"Scraping {title} topics: url = {url}")

    # File to store already scraped nodes
    SCRAPED_topics = f"Data/{title}/scraped_topics.json"

    # Load already scraped nodes from file
    def load_scraped_nodes():
        if os.path.exists(SCRAPED_topics):
            with open(SCRAPED_topics, "r") as file:
                return json.load(file)
        return {}  # Return empty dict if file doesn't exist

    # Save scraped nodes to file
    def save_scraped_nodes(scraped_nodes):
        with open(SCRAPED_topics, "w") as file:
            json.dump(scraped_nodes, file, indent=4)
            
    # Load already scraped nodes
    scraped_topics = load_scraped_nodes()
    if title in scraped_topics:
        print(f"Already scraped topics for {title}, skipping...")
        return scraped_topics[title]

    all_topics = []
    start = 0  # Start with the first page
    total_topics = 0  # Initialize total topics
    
    # First, collect all topic URLs and titles
    topic_collection = []
    
    seen_urls = set()
    while True:
        # Add the pagination parameter to the URL
        parts = url.split("-forum")
        # f"{parts[0]}p{start}-topic"
        paginated_url = f"{parts[0]}p{start}-forum"
        
        log_info(f"Fetching page with start={start}: {title} , {paginated_url}")

        soup = fetch_soup(paginated_url)
        if not soup:
            log_error(f"Failed to fetch {paginated_url}")
            break

        # if start == 0:
        #     pagination_div = soup.find('div', class_='pagination')
        #     if pagination_div:
        #         total_topics = int(pagination_div.get_text(strip=True).split()[0])
        #         log_info(f"Total topics for forum {title}: {total_topics}")
            
        # Find topics on the current page
        topics_list_outer = soup.find("ul", class_="topiclist topics bg_none")
        # print(topics_list_outer)
        # break
        
        
        if not topics_list_outer:
            log_info(f"No more topics found at start={start}")
            break

        topics_list = topics_list_outer.find_all("a", class_="topictitle")
        log_info(len(topics_list))
        if len(topics_list) <=0:
            log_info(f"Reached the last page at start={start}, This page has no topics and is empty")
            break
        
        if not topics_list:
            log_info(f"No topics found on page with start={start}")
            if total_topics > start +50:
                log_error(f"Expected more topics, but none found at start={start}") 
            break
        
        new_topics_found = False
        # Collect topics from the current page
        for topic in topics_list:
            topic_title = topic.get_text(strip=True)
            topic_url = urljoin(url, topic.get("href"))
            if topic_url in seen_urls:
                continue
            
            seen_urls.add(topic_url)
            new_topics_found = True
            topic_collection.append({
                "title": topic_title,
                "url": topic_url
            })
            
        if not new_topics_found:
            log_info(f"No new topics found at start={start}, ending loop to avoid repetition.")
            break

            
        # if start +50 >= total_topics:
        #     log_info(f"Completed scraping topics at start={start}")
        #     break

        #

        # Move to the next page
        start +=50

    log_info(f"Found {len(topic_collection)} topics to scrape from {title}")
    
    # Now scrape each topic in parallel
    log_info(f"Starting parallel scraping of {len(topic_collection)} topics with {max_workers} workers")
    
    # Create directory for this forum if it doesn't exist
    os.makedirs(f"Data/{title}", exist_ok=True)
    
    # Use ThreadPoolExecutor for parallel scraping
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create a mapping of futures to topics for tracking
        future_to_topic = {
            executor.submit(scrape_topic, topic["url"], topic["title"], title): topic 
            for topic in topic_collection
        }
        
        # Process results as they come in
        completed = 0
        len_posts = 0
        for future in as_completed(future_to_topic):
            topic = future_to_topic[future]
            try:
                topic_data = future.result()
                all_topics.append(topic_data)
                len_posts += len(topic_data["posts"])
                completed += 1
                
                if completed % 10 == 0 or completed == len(topic_collection):
                    log_info(f"Scraped {completed}/{len(topic_collection)} topics from {title}")
                
            except Exception as exc:
                log_error(f"Error scraping topic {topic['title']}: {exc}")

    log_info(f"Completed scraping {len(all_topics)} topics from {title} with {len_posts} posts")
    
    scraped_topics[title] = {
        "url": url,
        "scraped_at": str(datetime.datetime.now())  # Add timestamp
    }
    
    # Save updated scraped nodes
    save_scraped_nodes(scraped_topics)
    return len_posts




def scrape_topic(url, title, node_title):
    os.makedirs(f"Data/{node_title}/{title}", exist_ok=True)
    SCRAPED_topics = f"Data/{node_title}/{title}/scraped_topics.json"

    # Load already scraped nodes from file
    def load_scraped_nodes():
        if os.path.exists(SCRAPED_topics):
            with open(SCRAPED_topics, "r") as file:
                return json.load(file)
        return {}  # Return empty dict if file doesn't exist

    # Save scraped nodes to file
    def save_scraped_nodes(scraped_nodes):
        with open(SCRAPED_topics, "w") as file:
            json.dump(scraped_nodes, file, indent=4)
            
    # Load already scraped nodes
    scraped_topics = load_scraped_nodes()
    
    if title in scraped_topics:
        print(f"Already scraped topic {title}, skipping...")
        return scraped_topics[title]
    

    final_result = {
        "title": title,
        "url": url,
        "posts": []
    }

    start = 0  # Start with the first page
    completed = False
    while True:
        # Add the pagination parameter to the URL
        parts = url.split("-topic")
        # f"{parts[0]}p{start}-topic"
        paginated_url = f"{parts[0]}p{start}-topic"
        log_info(f"Fetching page: {start//25 + 1 }: {title}")

        soup = fetch_soup(paginated_url)
        # if start == 0:
        #     pagination_div = soup.find('div', class_='pagination')
        #     if pagination_div:
        #         total_posts = int(pagination_div.get_text(strip=True).split()[0])
        #         log_info(f"Total posts for forum {title}: {total_posts}")
                
        if not soup:
            log_error(f"Failed to fetch {title}")
            break

        # Find posts on the current page
        posts = soup.find_all('div', class_='postbody')
        log_info(f"Found {len(posts)} posts on page {start//25 + 1} of {title}")
        # print(posts)
        # if len(posts) <= 0:
        # Check if the current page has fewer than 10 posts (last page)
        if len(posts) <= 0 :
            # if start +25 < total_posts: 
            #     log_error(f"Expected more posts, but none found at start={start} for {title}")
            # else: 
            log_info(f"Completed scraping {title} at start={start}")
            completed = True
            break
        
        if not posts:
            log_error(f"No more posts found at start={start}")
            # if total_posts > start + 10:
            #     log_error(f"Expected more topics, but none found at start={start}") 
            break

        for post in posts:
            result = {}

            post_info = post.find('p', class_='author').text
            clean_text = unicodedata.normalize("NFKC", post_info).replace('\xa0', ' ').strip()
            pattern = r"από\s+(\S+)\s+(.+)$"
            match = re.search(pattern, post_info)
            if match:
                username = match.group(1)
                post_time = match.group(2)
                
            else:
                log_error(f"No match found for time and username. for {post_info}")
            
            if username == 'Χορηγούμενο':
                log_info(f"Skipping sponsored post by {username}")
                continue                        

            content = post.find('div', class_='content clearfix')
            
            content_final = extract_post_as_text(content)

            result["username"] = username
            result["post_time"] = post_time
            result["content"] = content_final
            final_result["posts"].append(result)

        
        
        # Move to the next page
        start += 25
        

    if completed:
        with open(f"Data/{node_title}/{title}/data.json", "w") as file:
            file.write(json.dumps(final_result, indent=4, ensure_ascii=False))
            
        # Save updated scraped nodes
        scraped_topics[title] = {
            "url": url,
            "scraped_at": str(datetime.datetime.now()),  # Add timestamp
        }
        save_scraped_nodes(scraped_topics)

    return final_result



def extract_post_as_text(content_div):
    if not content_div:
        return "Error: Content div not found"
    
    # String to store the formatted content
    formatted_text = ""
    
    # Process all elements in the content div
    process_node_to_text(content_div, formatted_text_list := [])
    
    # Join all text parts
    return "".join(formatted_text_list)

def process_node_to_text(node, text_parts):
    """
    Recursively process a node and its children to extract content as text
    
    Args:
        node: BeautifulSoup node
        text_parts: List to append text parts to
    """
    # Process all children of this node
    for child in node.children:
        if isinstance(child, NavigableString):
            # Text content
            text = child.strip()
            if text:
                text_parts.append(text + " ")
        elif child.name == 'br':
            # Line break
            text_parts.append("\n")
        elif child.name == 'img':
            # Image
            if 'smilies' in child.get('class', []):
                text_parts.append(f"smiley:{child.get('alt', '')} ")
            else:
                text_parts.append(f"img:{child.get('alt', '')} ")
        elif child.name == 'iframe':
            # Video iframe
            text_parts.append(f"video:{child.get('src', '')} ")
        elif child.name == 'a':
            # Link - just add the href
            text_parts.append(f"{child.get('href', '')} ")
        elif child.name == 'div' and 'video-container' in child.get('class', []):
            # Special case for video containers
            iframe = child.find('iframe')
            if iframe:
                text_parts.append(f"video:{iframe.get('src', '')} ")
        else:
            # Recursively process other elements
            process_node_to_text(child, text_parts)

def process_forum_page_to_text(html_content):
    """
    Process a forum page with multiple posts to text format
    
    Args:
        html_content (str): HTML content of the forum page
        
    Returns:
        list: List of text representations for each post
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find all posts
    posts = soup.find_all('div', class_='post')
    
    results = []
    for i, post in enumerate(posts):
        content_div = post.find('div', class_='content')
        if content_div:
            # Extract post content as text
            post_text = extract_post_as_text(str(content_div))
            
            # Add post metadata if available
            metadata = []
            
            author_div = post.find('div', class_='author')
            if author_div:
                author = author_div.get_text(strip=True)
                metadata.append(f"Author: {author}")
            
            date_div = post.find('div', class_='date')
            if date_div:
                date = date_div.get_text(strip=True)
                metadata.append(f"Date: {date}")
            
            # Format as a single post entry
            if metadata:
                formatted_post = f"--- POST #{i+1} ---\n{' | '.join(metadata)}\n\n{post_text}\n\n"
            else:
                formatted_post = f"--- POST #{i+1} ---\n{post_text}\n\n"
                
            results.append(formatted_post)
    
    return results