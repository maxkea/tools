import concurrent.futures
import os
import threading
import requests

# Biến toàn cục theo dõi tiến trình
downloaded_bytes = 0
lock = threading.Lock()


def get_file_size(url):
    response = requests.head(url, allow_redirects=True)
    return int(response.headers.get("content-length", 0))


def download_chunk(url, start, end, chunk_index, temp_files, total_size):
    global downloaded_bytes
    headers = {"Range": f"bytes={start}-{end}"}
    response = requests.get(url, headers=headers, stream=True)

    temp_filename = f"{OUTPUT_FILE}.part{chunk_index}"
    with open(temp_filename, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
                with lock:
                    downloaded_bytes += len(chunk)
                    percent = (downloaded_bytes / total_size) * 100
                    # Print cập nhật trên cùng 1 dòng
                    print(
                        f"\rĐã tải: {downloaded_bytes / (1024*1024):.2f} MB / {total_size / (1024*1024):.2f} MB ({percent:.1f}%)",
                        end="",
                    )

    temp_files[chunk_index] = temp_filename


def multi_thread_download(url, output_path, num_threads):
    file_size = get_file_size(url)
    chunk_size = file_size // num_threads
    temp_files = {}
    futures = []

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=num_threads
    ) as executor:
        for i in range(num_threads):
            start = i * chunk_size
            end = (
                (file_size - 1)
                if i == num_threads - 1
                else (start + chunk_size - 1)
            )
            futures.append(
                executor.submit(
                    download_chunk, url, start, end, i, temp_files, file_size
                )
            )

        concurrent.futures.wait(futures)

    print("\nĐang ghép file...")
    with open(output_path, "wb") as final_file:
        for i in range(num_threads):
            temp_filename = temp_files[i]
            with open(temp_filename, "rb") as part_file:
                final_file.write(part_file.read())
            os.remove(temp_filename)

    print("✅ Hoàn tất!")


if __name__ == "__main__":
    URL =str(input("nhap link download: "))
    OUTPUT_FILE= str(input("nhap ten file output: "))
    NUM_THREADS = int(input("nhap so thread: "))
    multi_thread_download(URL, OUTPUT_FILE, NUM_THREADS)