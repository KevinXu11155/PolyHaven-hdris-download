import requests
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from functools import partial
from retry import retry

@retry(tries=3, delay=2, backoff=2, max_delay=10, jitter=1)
def download_file(session, hdri, base_url, resolution, ext, save_dir):
    file_path = os.path.join(save_dir, f"{hdri}.{ext}")
    try:
        file_json = session.get(f"{base_url}/files/{hdri}").json()
        download_url = file_json["hdri"][resolution][ext]["url"]

        with session.get(download_url, stream=True, timeout=(30, 60)) as response:
            response.raise_for_status()
            total_size = int(response.headers.get('content-length', 0))

            with open(file_path, 'wb') as f, tqdm(
                desc=hdri[:15],
                total=total_size,
                unit='B',
                unit_scale=True,
                leave=False,
                bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]'
            ) as pbar:
                for chunk in response.iter_content(1024 * 1024):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))
        return True
    except Exception as e:
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"Cleaned incomplete file: {hdri}")
        
        if isinstance(e, KeyError):
            print(f"\n[ERROR] Format {ext} not available for: {hdri}")
        else:
            print(f"\n[FAILED] Download failed for {hdri}: {str(e)}")
        raise

def main():
    name, resolution, category, ext = sys.argv
    
    with requests.Session() as session:
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=20,
            pool_maxsize=20,
            max_retries=10
        )
        session.mount('https://', adapter)
        
        api_url = "https://api.polyhaven.com"
        hdris_url = f"{api_url}/assets?t=hdris"
        if category != "all":
            hdris_url += f"&c={category}"
        hdris = list(session.get(hdris_url).json().keys())

        save_dir = f"hdri_{resolution}"
        os.makedirs(save_dir, exist_ok=True)
        
        print(f"start downloading : {len(hdris)} hdris | resolution: {resolution} | extension: {ext}\n")

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = []
            download_func = partial(
                download_file, session, 
                base_url=api_url,
                resolution=resolution,
                ext=ext,
                save_dir=save_dir
            )
            
            for hdri in hdris:
                futures.append(executor.submit(download_func, hdri))
            
            success_count = 0
            with tqdm(total=len(futures), desc="Total Progress", unit='file') as main_pbar:
                for future in as_completed(futures):
                    try:
                        if future.result():
                            success_count += 1
                    except Exception as e:
                        pass
                    main_pbar.update(1)
                    
        print(f"\nDownload complete! Success: {success_count}/{len(hdris)}")

if __name__ == "__main__":
    main()
