from ftplib import FTP
import configparser
import os
import signal
import subprocess
import shutil
from send_mail import mail_send
from table_create import TableData, create_table
from dotenv import load_dotenv
from loguru import logger
import datetime
import pandas as pd
import xml.etree.ElementTree as ET
import csv
import argparse

class INAVUploader:
    def __init__(self, config_file_path):
        # Initialize the self.logger with rotation and retention policies
        logger.add(
            f"logs/iNAVUploader_{{time:YYYYMMDDHHmmss}}.log",  # Custom timestamp format in the filename
            rotation="5 MB",  # Rotate the file when it reaches 5MB
            retention="2 days",  # Keep log files for 2 days
            compression="zip",  # Compress old files to zip format
            format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <7} | {extra[id]: <5} | {message}"  # Include custom id
        )
        self.logger = logger

        load_dotenv()
        self.script_directory = os.path.dirname(os.path.abspath(__file__))
        self.download_path = f'{self.script_directory}/tmp'
        self.parser = configparser.ConfigParser()
        # config_file_path = os.path.join(self.script_directory, 'config/config.ini')
        self.parser.read(config_file_path)
        self.ftp = FTP(os.getenv('FTP_IP'))
        self.ftp.login(os.getenv('FTP_USERNAME'), os.getenv('FTP_PASSWORD'))
        self.table_data_list = []

    def get_latest_ftp_file_name(self, ftp_dir_path, file_pattern):
        self.logger.info(f"Checking Latest FTP file in {ftp_dir_path}")
        self.ftp.cwd(ftp_dir_path)
        files = self.ftp.nlst()
        if file_pattern != 'NoPattern':
            csv_files = [f for f in files if f.endswith('.csv') and f.startswith(file_pattern)]
        else:
            csv_files = [f for f in files if f.endswith('.csv')]
        latest_file = max(csv_files, key=lambda x: self.ftp.sendcmd('MDTM ' + x)[4:])
        self.logger.info(f"Latest FTP file = {latest_file}")
        return latest_file

    def get_local_file_name(self, local_dir_path):
        self.logger.info(f"Checking existing file in {local_dir_path}")
        files = os.listdir(local_dir_path)
        if len(files) == 1:
            file_name = files[0]
            self.logger.info(f"Existing file = {file_name}")
            return file_name
        elif len(files) > 1:
            self.logger.error(f"Multiple files found in {local_dir_path}. Please check!")
            return 'Multiple files found. Check directory!'
        else:
            self.logger.error(f"No file found in {local_dir_path}. Please check!")
            return 'FileNotFound'

    def ftp_file_download(self, latest_file, download_path):
        try:
            self.logger.info(f"Downloading {latest_file}")
            with open(os.path.join(download_path, latest_file), 'wb') as local_file:
                self.ftp.retrbinary("RETR " + latest_file, local_file.write)
            self.logger.info(f"File {latest_file} downloaded successfully")
            return 'Success'
        except Exception as e:
            self.logger.error(f'Error downloading file from FTP: {e}')
            return 'Failed'

    def file_diff(self, file1, file2):
        self.logger.info(f"Checking file diff between downloaded file and existing file")
        df1 = pd.read_csv(file1, encoding='ISO-8859-1')
        df2 = pd.read_csv(file2, encoding='ISO-8859-1')
        file1_name = os.path.basename(file1)
        file2_name = os.path.basename(file2)
        if df1.equals(df2):
            self.logger.info(f"No differences found between existing file : {file1_name} and downloaded_file : {file2_name}")
            return False
        else:
            self.logger.info(f"Differences found between existing file : {file1_name} and downloaded_file : {file2_name}")
            return True

    def get_isin_values(self, file_path, isin_key):
        tree = ET.parse(file_path)
        root = tree.getroot()
        for isin_key in root.iter(isin_key):
            value = isin_key.text
        self.logger.info(f"{isin_key} = {value}")
        return value

    def check_isin_include(self, file, config_file, isin_key):
        self.logger.info(f"Check {isin_key} included in downloaded file")
        value = self.get_isin_values(config_file, isin_key)
        with open(file, mode='r', encoding='ISO-8859-1', newline='') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if value in row:
                    self.logger.info(f"{isin_key}={value} found.")
                    return True
        self.logger.warning(f"{isin_key}={value} not found.")
        return False

    def backup_existing_file(self, file, backup_dir):
        file_name = os.path.basename(file)
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
            self.logger.info(f"Backup directory {backup_dir} created.")
        try:
            shutil.move(file, backup_dir)
            self.logger.info(f"Existing file : {file_name} moved to {backup_dir} successfully.")
            return 'Success'
        except Exception as e:
            self.logger.error(f"Error occurred while moving file to backup: {e}")
            return 'Failed'

    def update_latest_file(self, file, dest_dir):
        file_name = os.path.basename(file)
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir)
            self.logger.info(f"Destination directory {dest_dir} created.")
        try:
            shutil.move(file, dest_dir)
            self.logger.info(f"Downloaded file : {file_name} moved to {dest_dir} successfully.")
            return 'Success'
        except Exception as e:
            self.logger.error(f"Error occurred while moving file to destination: {e}")
            return 'Failed'

    def delete_file(self, file_path):
        file_name = os.path.basename(file_path)
        try:
            os.remove(file_path)
            self.logger.info(f"Downloaded file : {file_name} has been deleted successfully.")
        except FileNotFoundError:
            self.logger.error(f"File {file_path} not found.")
        except PermissionError:
            self.logger.error(f"Permission denied to delete file: {file_path}.")
        except Exception as e:
            self.logger.error(f"Error occurred while deleting file: {e}")

    def kill_component(self, tag):
        try:
            result = subprocess.run(f"ps x | grep -w {tag} | grep -v grep", shell=True, stdout=subprocess.PIPE, text=True)
            lines = result.stdout.strip().split('\n')
            pid_list = [int(line.split(None, 1)[0]) for line in lines if line]
            if not pid_list:
                self.logger.info(f"No process found with the tag '{tag}'.")
                return
            if len(pid_list) > 1:
                self.logger.warning(f"Multiple processes found with the tag '{tag}': {', '.join(map(str, pid_list))}")
            else:
                pid = pid_list[0]
                os.kill(pid, signal.SIGKILL)
                self.logger.info(f"Process {pid} with tag '{tag}' has been terminated (SIGKILL).")
        except Exception as e:
            self.logger.error(f"An error occurred while killing the component: {e}")

    def inav_update(self, id, file_key, isin_key, file_pattern):
        ftp_dir_path = self.parser[id][file_key]
        component_path = self.parser[id]['component_path']
        local_dir_path = f'{component_path}/{file_key}'
        latest_ftp_file_name = self.get_latest_ftp_file_name(ftp_dir_path, file_pattern)
        local_file_name = self.get_local_file_name(local_dir_path)
        status = ''
        download_status = self.ftp_file_download(latest_ftp_file_name, self.download_path)   
        if download_status == 'Success':
            downloaded_file = f'{self.download_path}/{latest_ftp_file_name}'
            if local_file_name == 'FileNotFound':
                self.logger.warning(f"{id} {file_key} local file not found. Proceeding with the file update.")
                config_file = f'{component_path}/config/CacheConfig.xml'
                if self.check_isin_include(downloaded_file, config_file, isin_key):
                    backup_dir = f'{component_path}/backupCSV/'
                    file_update_status = self.update_latest_file(downloaded_file, local_dir_path)
                    if file_update_status == 'Success':
                        status = 'Updated'
                        description = 'File updated successfully (local file not found)'
                    else:
                        status = 'Failed'
                        description = 'File Update Failed'
                else:
                    status = 'Failed'
                    description = 'ISIN not Found'
            else:
                local_file = f'{local_dir_path}/{local_file_name}'
                if (self.file_diff(local_file, downloaded_file)) or (latest_ftp_file_name != local_file_name):
                    config_file = f'{component_path}/config/CacheConfig.xml'
                    if self.check_isin_include(downloaded_file, config_file, isin_key):
                        backup_dir = f'{component_path}/backupCSV/'
                        backup_status = self.backup_existing_file(local_file, backup_dir)
                        if backup_status == 'Success':
                            file_update_status = self.update_latest_file(downloaded_file, local_dir_path)
                            if file_update_status == 'Success':
                                status = 'Updated'
                                description = 'File updated successfully'
                            else:
                                status = 'Failed'
                                description = 'File Update Failed'
                        else:
                            status = 'Failed'
                            description = 'Backup Failed'
                    else:
                        status = 'Failed'
                        description = 'ISIN not Found'
                else:
                    status = 'Not Updated'
                    description = 'No Differences in Files'
        else:
            status = 'Failed'
            description = 'File Download Failed'
        
        self.logger.info(f"iNAV update result: Status={status}, Description={description}")
        self.table_data_list.append(TableData(component_name=id, file_category=file_key, old_file=local_file_name, new_file=latest_ftp_file_name, status=status, description=description))

        if status == 'Updated':
            return True
        else:
            self.delete_file(downloaded_file)
            return False

    def run(self):
        for id in self.parser.sections():
            self.logger = self.logger.bind(id=id)
            self.logger.info(f"Processing {id}")
            update_count = 0
            file_types = [
                {'iNAVFile': 'iNAVSymbolISIN'},
                {'iNAVCurrencyConvFile': 'iNAVConversionCurrencyISIN'}
            ]

            for file_type in file_types:
                for file_key, isin_key in file_type.items():
                    if self.parser.has_option(id, file_key):
                        file_pattern_key = f'{file_key}Pattern'
                        if self.parser.has_option(id, file_pattern_key):
                            file_pattern = self.parser[id][file_pattern_key]
                        else:
                            file_pattern = 'NoPattern'
                        self.logger.info(f"Started updating {id} {file_key}")
                        if self.inav_update(id, file_key, isin_key, file_pattern):
                            update_count += 1

            
            if update_count >= 1:
                self.logger.info(f"{id} files have been updated. Restarting the component.")
                tag = self.parser[id]['tag']
                self.kill_component(tag)

        if mail_send(create_table(self.table_data_list)):
            self.logger.info("Mail sent successfully.")
        else:
            self.logger.error(f"Error in mail send")

if __name__ == '__main__':
    # Argument parsing to accept the config file path
    parser = argparse.ArgumentParser(description='INAVUploader script')
    parser.add_argument('--config', required=True, help='Path to the configuration file')

    # Parse the arguments
    args = parser.parse_args()
    config_file_path = args.config
    if not os.path.exists(config_file_path):
            raise FileNotFoundError(f"Config file '{config_file_path}' does not exist.")     
    
    # Initialize and run the uploader with the provided config file path
    inav_uploader = INAVUploader(config_file_path)
    inav_uploader.run()
