import sys
import os
import csv
import json

def read_file(file_path):
    if not os.path.exists(file_path):
        return f"Error: File not found: {file_path}"

    _, ext = os.path.splitext(file_path)
    ext = ext.lower()

    try:
        if ext == '.csv':
            # Try using pandas if available for better formatting, else fallback to csv module
            try:
                import pandas as pd
                df = pd.read_csv(file_path)
                return df.to_markdown(index=False)
            except ImportError:
                with open(file_path, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    rows = list(reader)
                    return "\n".join([",".join(row) for row in rows])
        
        elif ext in ['.md', '.txt', '.json']:
             with open(file_path, 'r', encoding='utf-8') as f:
                return f.read()
        
        else:
            return f"Error: Unsupported file extension: {ext}"

    except Exception as e:
        return f"Error reading file {file_path}: {e}"

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python read_data.py <file_path>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    print(read_file(file_path))
