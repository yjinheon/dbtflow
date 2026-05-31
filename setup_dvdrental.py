import requests


def download_dvdrental():
    url = "https://raw.githubusercontent.com/robconery/dvdrental/master/dvdrental.tar"
    filename = "dvdrental.tar"

    print(f"Downloading {url}...")
    response = requests.get(url, stream=True)
    
    if response.status_code == 200:
        with open(filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"'{filename}' 파일이 성공적으로 다운로드되었습니다.")
    else:
        print(f"다운로드 실패. 상태 코드: {response.status_code}")
        return

    print("\nPostgreSQL에서 다음 명령어로 복원:")
    print(f"pg_restore -U postgres -d dvdrental {filename}")


if __name__ == "__main__":
    download_dvdrental()
