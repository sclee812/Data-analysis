"""
    backup_manager.py

    author: Terry Lee
    created: 28/1/2022
    updated: 31/1/2022
"""

import os
import re
from datetime import datetime, timezone

class BackupManager():
    def __init__(self):
        pass

    @staticmethod
    def save_dataframe_to_csv(df, f_name, index = False):
        if os.path.isfile(f_name):
            if BackupManager.change_file_name_with_modified_date(f_name):
                df.to_csv(f_name, encoding='utf-8-sig', index = index)
        else:
            df.to_csv(f_name, encoding='utf-8-sig', index = index)

    @staticmethod
    def change_file_name_with_modified_date(f_name):
        with_date = BackupManager.get_file_modified_date(f_name)
        dot_pos = f_name.rfind('.')

        modified = with_date
        for count in range(5):
            if count > 0:
                modified = with_date + '_' + str(count)
            new_name = f_name[:dot_pos] + '_' + modified + f_name[dot_pos:]
            print(new_name)
            try:
                os.rename(f_name, new_name)
                return True
            except FileExistsError:
                pass

        print("File already exists with name: {}. Please change the file name manually and try again".format(new_name))
        return False

    @staticmethod
    def get_file_modified_date(file_path):
        folder_name = os.path.dirname(file_path)
        if folder_name == '':
            folder_name = '.'
        with os.scandir(folder_name) as dir_entries:
            for entry in dir_entries:
                if os.path.basename(file_path) in str(entry.name):
                    modified = datetime.fromtimestamp(entry.stat().st_mtime, tz=timezone.utc)
                    print(entry, modified.strftime("%Y%m%d")[:8])
                    return modified.strftime("%Y%m%d")[:8]

        return ''

    @staticmethod
    def change_file_names_in_folder(folder_name):
        with os.scandir(folder_name) as dir_entries:
            for entry in dir_entries:
                print(entry.name)
                if not BackupManager.file_name_has_modified_date(entry.name):
                    BackupManager.change_file_name_with_modified_date(folder_name + r'\\' + entry.name)
                else:
                    print("Already has modified date in file name: {}".format(entry.name))

    @staticmethod
    def file_name_has_modified_date(file_path):
        result = re.findall(r'_(\d{8})\.', file_path)
        if len(result) > 0:

            date_in_name = result[0]
            actual_date = BackupManager.get_file_modified_date(file_path)
            if date_in_name == actual_date:
                return True
            else:
                print("Date found with {}, but not the actual date ({})".format(date_in_name, actual_date))

        return False

    @staticmethod
    def get_last_backed_up_file_name(file_path):
        files_dict = {}
        folder_name = os.path.dirname(file_path)
        if folder_name == '':
            folder_name = '.'

        file_name = os.path.basename(file_path)
        file_name_without_ext = file_name[:file_name.rfind('.')]
        with os.scandir(folder_name) as dir_entries:
            for entry in dir_entries:
                if file_name_without_ext in str(entry.name):
                    files_dict[entry.name] = datetime.fromtimestamp(entry.stat().st_mtime, tz=timezone.utc)

        files_dict_sorted = {k: v for k, v in sorted(files_dict.items(), key=lambda item: item[1])}
        if len(files_dict_sorted) < 2:
            print("No backup file found")
            return ''

        return BackupManager.get_nth_key(files_dict_sorted, n = -2)

    @staticmethod
    def get_nth_key(dict, n = 0):
        if n < 0:
            n += len(dict)
        for i, key in enumerate(dict.keys()):
            if i == n:
                return key

        raise IndexError("dictionary index out of range")
