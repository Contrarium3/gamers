import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import json

import socket
import time

BASE_URL = "https://greekgamerz.forumgreek.com/forum"

headers = {
    "User-Agent": "Mozilla/5.0"
}

class ForumNode:
    def __init__(self, title, url):
        self.title = title
        self.url = url
        self.children = []
        self.has_topics = False

    def is_leaf(self):
        return self.has_topics and not self.children

    def to_dict(self):
        return {
            "title": self.title,
            "url": self.url,
            "has_topics": self.has_topics,
            "children": [child.to_dict() for child in self.children]
        }

    @classmethod
    def from_dict(cls, data):
        node = cls(data["title"], data["url"])
        node.has_topics = data["has_topics"]
        node.children = [cls.from_dict(child) for child in data["children"]]
        return node

    def __repr__(self, level=0):
        indent = "  " * level
        result = f"{indent}- {self.title} ({'topics' if self.has_topics else 'no topics'})\n"
        for child in self.children:
            result += child.__repr__(level + 1)
        return result
    

def is_connected(host="8.8.8.8", port=53, timeout=3):
    """Check if there's internet connection by trying to reach a DNS server."""
    try:
        socket.setdefaulttimeout(timeout)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, port))
        return True
    except socket.error:
        return False

def fetch_soup(url, retry_interval=3):
    while True:
        if is_connected():
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                return BeautifulSoup(response.text, "html.parser")
            except Exception as e:
                print(f"Failed to fetch {url}: {e}")
                print(f"Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
        else:
            print("No internet connection. Waiting for connection...")
            time.sleep(retry_interval)
            

def get_subforum_links(soup):
    links = []
    for a in soup.select("a.forumtitle"):
        href = a.get("href")
        full_url = urljoin(BASE_URL, href)
        title = a.get_text(strip=True)
        links.append((title, full_url))
    return links

def has_topics(soup):
    return bool(soup.select("a.topictitle"))

def build_tree(url, title="Root" ):


    node = ForumNode(title, url)
    soup = fetch_soup(url)
    if not soup:
        return node

    node.has_topics = has_topics(soup)

    subforums = get_subforum_links(soup)
    for sub_title, sub_url in subforums:
        print(f"Visiting subforum: {sub_title} -> {sub_url}")
        child_node = build_tree(sub_url, sub_title)
        if child_node:
            node.children.append(child_node)

    return node




if __name__ == "__main__":
    tree = build_tree(BASE_URL)
    print(tree)
    with open("forum_tree.json", "w", encoding="utf-8") as f:
        json.dump(tree.to_dict(), f, ensure_ascii=False, indent=2)
    