import zipfile
import os

def unzip(zip_path, extract_to='data'):
    # Ensure the target directory exists
    os.makedirs(extract_to, exist_ok=True)
    # Extract the ZIP file
    print("Extracting files...")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    print(f"Files extracted to {extract_to}")
    # os.remove(zip_path)

if __name__ == "__main__":
    input_dir = "/opt/ml/processing/input"
    output_dir = "/opt/ml/processing/output"
    print(os.listdir(input_dir))

    zip_path = os.path.join(input_dir, 'dataset.zip')
    unzip(zip_path, output_dir)