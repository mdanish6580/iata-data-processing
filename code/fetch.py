import requests
import os

def fetch(url, extract_to='data'):
    # Ensure the target directory exists
    os.makedirs(extract_to, exist_ok=True)
    
    # Define headers to mimic a browser request
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
    }
    
    # Download the file
    zip_path = os.path.join(extract_to, 'dataset.zip')
    print(f"Downloading from {url}...")
    response = requests.get(url, headers=headers, stream=True)
    response.raise_for_status()
    with open(zip_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=1024):
            f.write(chunk)
    print("Download completed.")

if __name__ == "__main__":
    url = "https://eforexcel.com/wp/wp-content/uploads/2020/09/2m-Sales-Records.zip"
    output_dir = "/opt/ml/processing/output"
    fetch(url, output_dir)