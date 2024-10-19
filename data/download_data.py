"""download_data.py: Downloads the yellow and green taxi data from years 2023 and 2024 from a Google Drive link.
The data is then stored in the raw_data directory.

10/19/2024
"""

import gdown, os  

FILE_IDS = {
    'yellow': "1hiuzsDCQglNogfK6-O9DKFVzL3hZfZB6",
    'green': "1dIZtze5fTjnX3ZUVHl5Ddf4NzovBycFR"
}


def download_script(color: str):
    """
    Downloads the yellow and green taxi data from years 2023 and 2024 from a Google Drive link.
    The data is then stored in the raw_data directory.

    :param color: The color of the taxi.
    """
    pwd = os.path.dirname(os.path.abspath(__file__))  # Get the current working directory.

    # Create the raw_data directory if it doesn't exist.
    raw_data_dir = os.path.join(pwd, 'raw_data')
    if not os.path.exists(raw_data_dir):
        os.makedirs(raw_data_dir)

    # Download the data from the Google Drive link.
    file_id = FILE_IDS[color]
    url = f'https://drive.google.com/uc?id={file_id}'  # The Google Drive link.
    output = os.path.join(raw_data_dir, f'{color}.parquet')  # The output file path. Save the data as a parquet file.

    gdown.download(url, output, quiet=False)  # Download the data. Quiet is set to False to show the download progress.

    print(f'{color} taxi data downloaded successfully.')


def main():
    """
    Downloads the yellow and green taxi data if not already downloaded.
    """
    # check if the yellow and green taxi data is already downloaded.
    pwd = os.path.dirname(os.path.abspath(__file__))
    yellow_file = os.path.join(pwd, 'raw_data', 'yellow.parquet')
    green_file = os.path.join(pwd, 'raw_data', 'green.parquet')

    if not os.path.exists(yellow_file):
        download_script('yellow')
    else:
        print('Yellow taxi data already downloaded.')
    
    if not os.path.exists(green_file):
        download_script('green')
    else:
        print('Green taxi data already downloaded.')
    
    print('All data downloaded successfully.')


if __name__ == '__main__':
    main()
