import os
import time
import requests
from bs4 import BeautifulSoup
import boto3
from urllib.parse import urlparse
import random
from urllib.parse import urljoin

def get_random_user_agent():
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    ]
    return random.choice(user_agents)


def get_internal_links(url):
    headers = {
        'User-Agent': get_random_user_agent()
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        internal_links = set()
        for link in soup.find_all('a' or 'link', href=True):
            absolute_url = urljoin(url, link['href'])
            if is_internal_link(url, absolute_url):
                internal_links.add(absolute_url)
        return internal_links
    else:
        print("Failed to fetch page:", response.status_code)
        return set()


# Function to check if a link is internal
def is_internal_link(base_url, link):
    return link.startswith(base_url)


def get_all_pages(base_url):
    visited = set()
    queue = [base_url]

    while queue:
        url = queue.pop(0)

        if url not in visited:
            visited.add(url)
            internal_links = get_internal_links(url)
            queue.extend(internal_links)
            time.sleep(random.uniform(1, 3))  # Add a delay between requests
    return visited


# Function to scrape text and images from a page
def scrape_page(page_url):
    headers = {
        'User-Agent': get_random_user_agent()
    }
    response = requests.get(page_url, headers=headers)

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        # Extract text content
        text_content = soup.get_text(separator='\n')
        # Extract image URLs
        image_urls = [img['src'] for img in soup.find_all('img')]
        return text_content, image_urls
    else:
        print(f"Failed to fetch page: {page_url}")
        return None, []

#Function to Download Images
def download_image(url, output_dir):
    response = requests.get(url, headers={'User-Agent': get_random_user_agent()})
    if response.status_code == 200:
        filename = os.path.basename(url)
        with open(os.path.join(output_dir, filename), 'wb') as f:
            f.write(response.content)
        print(f"Image saved: {filename}")
    else:
        print(f"Failed to download image: {url}")

#Create FileNames for Text Files
def get_text_file_name(page_url):
    global count
    if page_url != ' ' or page_url != '':
        # Upload text content to S3
        parsed_url = urlparse(page_url)
        path_components = parsed_url.path.split('/')
        if path_components != ' ' or path_components != '':
            text_file_name = path_components[-1]
            if text_file_name:
                text_file_name = 'scraped_content/text/' + text_file_name + '.txt'
                return text_file_name
            else:
                text_file_name = 'scraped_content/text/page_no_' + str(count) + text_file_name
                count = count + 1
                return text_file_name
    else:
        return None

#Create FileNames for Image Files
def get_image_file_name(image_url):
    count = 0
    if image_url != ' ' or image_url != '':
        # Upload text content to S3
        parsed_url = urlparse(image_url)
        path_components = parsed_url.path.split('/')
        if path_components != ' ' or path_components != '':
            image_file_name = path_components[-1]
            if image_file_name:
                image_file_name = '/images/' + image_file_name
                return image_file_name
            else:
                image_file_name = 'page_no_' + str(count) + image_file_name
                count = count + 1
                return image_file_name
    else:
        return None

#Upload files to S3 Bucket
def upload_data_to_s3(data, bucket_name, object_name):
    # Create an S3 client
    try:
        # Upload the data
        response = s3_client.put_object(Body=data, Bucket=bucket_name, Key=object_name)
    except Exception as e:
        print(f"Error uploading data to bucket '{bucket_name}' with key '{object_name}': {str(e)}")
        return False
    else:
        print(f"Data uploaded successfully to bucket '{bucket_name}' with key '{object_name}'")
        return True

if __name__ == '__main__':
    output_dir = 'scraped_content'
    base_url = 'https://franchisesuppliernetwork.com'
    bucket_name = 'franchisesuppliernetwork'
    all_pages = get_all_pages(base_url)
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    files = [obj['Key'] for obj in response.get('Contents', [])]
    global count
    count = 0
    for page_url in all_pages:
        text_content, image_urls = scrape_page(page_url)
        if text_content:
            if page_url != ' ' or page_url != '':
                text_file_name = get_text_file_name(page_url)
                upload_data_to_s3(text_content, bucket_name, text_file_name)
    for page_url in all_pages:
        text_content, image_urls = scrape_page(page_url)
        for idx, image_url in enumerate(image_urls):
            if image_url:
                image_response = requests.get(image_url)
                if image_response.status_code == 403:
                    image_file_name = get_image_file_name(image_url)
                    full_path = output_dir + image_file_name
                    upload_data_to_s3(image_response.content, bucket_name, full_path)