import os
import sys
import requests
from zipfile import ZipFile

def download_file(url, output_dir):
   
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    local_filename = os.path.join(output_dir, "dataset.zip")
    response = requests.get(url, stream=True)

    if response.status_code == 200:
        with open(local_filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        print(f"[INFO] File downloaded successfully: {local_filename}")
        return local_filename
    else:
        raise Exception(f"Download failed. Status code: {response.status_code}")

def extract_file(zip_file, output_dir):
    
    with ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(output_dir)
    print(f"[INFO] Files extracted to: {output_dir}")

    os.remove(zip_file)
    print(f"[INFO] Deleted temporary zip file: {zip_file}")

def fix_json_format(json_file):
    
    import json
    with open(json_file, 'r') as f:
        data = json.load(f)

    fixed_file = json_file.replace(".json", "_fixed.json")
    with open(fixed_file, 'w') as f:
        for key, value in data.items():
            record = {key: value}
            f.write(json.dumps(record) + "\n")

    os.remove(json_file)
    print(f"[INFO] Fixed JSON saved as: {fixed_file}")
    return fixed_file

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python execute.py <extract_path>")
        sys.exit(1)

    EXTRACT_PATH = sys.argv[1]
    KAGGLE_URL = "https://storage.googleapis.com/kaggle-data-sets/5259429/8820618/bundle/archive.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20250825%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20250825T105953Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=908ceafccfe9a7504e164b47ba7bd38bb3e654c43fac30c2b3ae8ad81efcc74ee71aa3779927d8efc2973f8a0c8d42a2f24d4d5a02542864133004d5f46c815f15b4d7b300b78c7de157e0d8455e94ac8bc3ee1dc7bb5f5374c69f1d2bb06448920a2f33aec86a61a9d813a118ec95e8d054bad1edff5e6fa697d9ea797c7fa6b2716bb53cd4a49aea3f5234d2892a8e377f5e5772d5f478edc362083bc67cab3e9609669431df555dcd8dd75079e7385f9ef243d61e9c389f4d30fd4abc422a11c4d99872696f2cc0970a19e4929015d341b37401f7aa1ef10b0ade5aa773adc2c070175acf507e1605925447948ad9dec90035ee4a68ce7c7fc3a3859a0d01"

    try:
        zip_file = download_file(KAGGLE_URL, EXTRACT_PATH)
        extract_file(zip_file, EXTRACT_PATH)
        print("[INFO] Extraction completed successfully.")
    except Exception as e:
        print(f"[ERROR] {str(e)}")
