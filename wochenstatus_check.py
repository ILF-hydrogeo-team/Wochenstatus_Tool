# -*- coding: utf-8 -*-
"""
Created on Thu Mar 20 15:47:23 2025

@author: Lola.Neuert
"""

import os
import pandas as pd
import re

def search_in_excel_files(main_folder, search_pattern):
    found_files = []
    pattern = re.compile(search_pattern)
    
    for root, _, files in os.walk(main_folder):
        for file in files:
            if file.endswith(('.xlsx', '.xls')):  # Check for Excel files
                file_path = os.path.join(root, file)
                try:
                    xls = pd.ExcelFile(file_path)
                    for sheet_name in xls.sheet_names:
                        df = pd.read_excel(xls, sheet_name=sheet_name, dtype=str)  # Read as string to avoid type issues
                        if df.apply(lambda x: x.astype(str).str.contains(pattern, na=False)).any().any():
                            relative_path = os.path.relpath(file_path, main_folder)  # Get relative path
                            found_files.append(f"{relative_path}")
                            break  # Stop checking after the first match
                except Exception as e:
                    print(f"Error reading: {e}")
                    
    print("found in files:")
    for file in found_files:
        print(file)
    
    return found_files

if __name__ == "__main__":
    main_folder = input("Enter the main folder path: ").strip().strip('"')
    search_term = input("Enter the search pattern (e.g., PA9-BK-Gro-0000): ").strip()
    search_in_excel_files(main_folder, re.escape(search_term))
    
print("processing complete")
