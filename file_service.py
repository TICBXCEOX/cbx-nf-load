import os
import shutil
import uuid
import zipfile

from os import makedirs
from os.path import join

class FileService:
    DEFAULT_DIR = 'zip'
    
    def __init__(self):
        self = self

    def get_path_dir_extract(self, file_id):
        return join('extract', self.DEFAULT_DIR, file_id)

    def create_dir_extract(self, file_id):
        filepath_to_extract = self.get_path_dir_extract(file_id)
        makedirs(filepath_to_extract, exist_ok=True)
        return filepath_to_extract

    def remove(self, path):
        shutil.rmtree(path)
        os.remove(path)
    
    def extract_zip(self, zip_dir):
        path = uuid.uuid4()
        extract_dir = self.create_dir_extract(str(path))
        for root, dirs, files in os.walk(zip_dir):
            for file in files:
                if file.endswith('.zip'):
                    zip_file = os.path.join(root, file)
                    self.extract_nested_zips(zip_file, extract_dir)
        return extract_dir        
    
    def extract_nested_zips(self, zip_file, extract_dir):
        """
        Extract contents of nested zip files recursively.
        
        Parameters:
        - zip_file: Path to the zip file to extract.
        - extract_dir: Directory where contents will be extracted.
        """
        with zipfile.ZipFile(zip_file, 'r') as zf:            
            zf.extractall(path=extract_dir)
            print(f'Extracting {str(zip_file)}')
            for file in zf.namelist():
                if file.lower().endswith('.zip'):                    
                    nested_zip_path = os.path.join(extract_dir, file)
                    self.extract_nested_zips(nested_zip_path, extract_dir)