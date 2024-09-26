import pandas as pd
import os
import argparse
from datetime import datetime
import logging
from tqdm import tqdm

def standardize_column_names(df):
    new_columns = []
    for col in df.columns:
        if isinstance(col, str):
            if 'code' in col.lower():
                new_columns.append('Employee Code')
            elif 'name' in col.lower():
                new_columns.append('Employee Name')
            else:
                try:
                    date = pd.to_datetime(col, format='%d-%m-%Y', errors='raise')
                    new_columns.append(date.strftime('Status (%d-%b-%y)'))
                except ValueError:
                    try:
                        date = pd.to_datetime(col, format='%d-%b-%y', errors='raise')
                        new_columns.append(date.strftime('Status (%d-%b-%y)'))
                    except ValueError:
                        new_columns.append(col)
        elif isinstance(col, datetime):
            new_columns.append(col.strftime('Status (%d-%b-%y)'))
        else:
            new_columns.append(col)
    
    df.columns = new_columns
    return df

def load_file(file_path, sheet_name=None):
    file_extension = os.path.splitext(file_path)[1].lower()
    
    try:
        if file_extension in ['.xls', '.xlsx', '.xlsm']:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
        elif file_extension == '.csv':
            df = pd.read_csv(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_extension}")
        
        logging.info(f"Columns in the loaded dataframe: {df.columns.tolist()}")
        return df
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        raise
    except PermissionError:
        logging.error(f"Permission denied when trying to access file: {file_path}")
        raise
    except Exception as e:
        logging.error(f"Error loading file {file_path}: {str(e)}")
        raise

def process_dataframe(df, id_col='Employee Code', name_col='Employee Name'):
    df = standardize_column_names(df)
    
    if id_col not in df.columns:
        logging.warning(f"Warning: '{id_col}' column not found. Available columns: {df.columns.tolist()}")
        id_col = input("Please enter the correct column name for Employee ID: ").strip()
        
        # Check if the user input is valid
        while id_col not in df.columns:
            logging.error(f"Error: '{id_col}' is not a valid column name.")
            id_col = input("Please enter a valid column name for Employee ID: ").strip()
    
    df.set_index(id_col, inplace=True)
    
    date_columns = [col for col in df.columns if col.startswith('Status')]
    
    for col in date_columns:
        try:
            date_str = col.split('(')[1].split(')')[0]
            date = pd.to_datetime(date_str, format='%d-%b-%y')
            new_col_name = date.strftime('%Y-%m-%d')
            df.rename(columns={col: new_col_name}, inplace=True)
        except (IndexError, ValueError):
            logging.warning(f"Warning: Unable to parse date from column '{col}'. Keeping original name.")
    
    # Convert date columns to string to maintain consistency
    for col in df.columns:
        if col != name_col:
            df[col] = df[col].astype(str)
    
    return df

def load_backend_data(file_path, sheet_name):
    df = load_file(file_path, sheet_name)
    logging.info(f"Loaded backend data from {file_path}, sheet: {sheet_name}")
    return process_dataframe(df)

def load_new_attendance(file_path):
    try:
        df = load_file(file_path, "Attn")
        logging.info(f"Loaded new attendance data from {file_path}, sheet: Attn")
    except ValueError:
        logging.warning(f"Warning: 'Attn' sheet not found in {file_path}. Using the first sheet.")
        df = load_file(file_path)
        logging.info(f"Loaded new attendance data from {file_path}, first sheet")
    
    return process_dataframe(df)

def compare_attendance(backend_data, new_attendance):
    report_entries = []
    
    for emp_id in tqdm(new_attendance.index, desc="Processing employees"):
        if emp_id in backend_data.index:
            emp_name = new_attendance.at[emp_id, 'Employee Name']
            
            for date in new_attendance.columns:
                if date == 'Employee Name':
                    continue
                
                attn_value = new_attendance.at[emp_id, date]
                qandle_value = backend_data.at[emp_id, date] if date in backend_data.columns else 'N/A'
                
                # Determine if there's a mismatch
                if (attn_value == 'nan' or pd.isna(attn_value)) and (qandle_value == 'nan' or pd.isna(qandle_value)):
                    mismatch = 'No'
                    attn_display = ''
                    qandle_display = ''
                elif attn_value != qandle_value:
                    mismatch = 'Yes'
                    attn_display = attn_value if attn_value != 'nan' else ''
                    qandle_display = qandle_value if qandle_value != 'nan' else ''
                else:
                    mismatch = 'No'
                    attn_display = attn_value if attn_value != 'nan' else ''
                    qandle_display = qandle_value if qandle_value != 'nan' else ''
                
                # Convert date back to desired format for reporting
                try:
                    date_obj = pd.to_datetime(date)
                    date_str = date_obj.strftime('%d-%b-%y')
                except:
                    date_str = date
                
                report_entries.append({
                    "Emp ID": emp_id,
                    "Emp Name": emp_name,
                    "Date": date_str,
                    "Attn": attn_display,
                    "Qandle": qandle_display,
                    "Mismatch": mismatch
                })
        else:
            emp_name = new_attendance.at[emp_id, 'Employee Name'] if 'Employee Name' in new_attendance.columns else 'N/A'
            for date in new_attendance.columns:
                if date == 'Employee Name':
                    continue  # Skip the Employee Name column
                
                attn_value = new_attendance.at[emp_id, date]
                
                # Convert date back to desired format for reporting
                try:
                    date_obj = pd.to_datetime(date)
                    date_str = date_obj.strftime('%d-%b-%y')
                except:
                    date_str = date
                
                report_entries.append({
                    "Emp ID": emp_id,
                    "Emp Name": emp_name,
                    "Date": date_str,
                    "Attn": attn_value if not (attn_value == 'nan' or pd.isna(attn_value)) else '',
                    "Qandle": 'Employee not found in backend',
                    "Mismatch": 'Yes'
                })
    
    return report_entries

def generate_report(report_entries):
    if not report_entries:
        logging.info("No data to generate report.")
        return

    report_df = pd.DataFrame(report_entries)
    
    # Reorder columns to match the requested format
    report_df = report_df[["Emp ID", "Emp Name", "Date", "Attn", "Qandle", "Mismatch"]]
    
    report_file = "attendance_discrepancy_report.xlsx"
    report_df.to_excel(report_file, index=False)
    logging.info(f"Discrepancy report generated: {report_file}")
    total_mismatches = report_df[report_df["Mismatch"] == "Yes"].shape[0]
    logging.info(f"Total mismatches found: {total_mismatches}")

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    parser = argparse.ArgumentParser(description="Attendance Reconciliation Tool")
    parser.add_argument("--file", help="Path to the new attendance file")
    parser.add_argument("--config", help="Path to the configuration file", default="config.ini")
    args = parser.parse_args()

    backend_file = next((os.path.join("backend", f"Qandle{ext}") 
                         for ext in ['.xlsx', '.xlsm', '.xls', '.csv'] 
                         if os.path.exists(os.path.join("backend", f"Qandle{ext}"))), 
                        None)
    
    if not backend_file:
        logging.error("Backend file not found in the 'backend' directory.")
        raise FileNotFoundError("Backend file not found in the 'backend' directory.")
    
    logging.info(f"Found backend file: {backend_file}")
    
    if args.file:
        new_attendance_file = args.file
    else:
        new_attendance_file = input("Enter the path to the new attendance file: ").strip()
    
    if not os.path.exists(new_attendance_file):
        logging.error(f"New attendance file not found: {new_attendance_file}")
        raise FileNotFoundError(f"New attendance file not found: {new_attendance_file}")

    try:
        backend_data = load_backend_data(backend_file, sheet_name="Qandle")
        new_attendance = load_new_attendance(new_attendance_file)
        report_entries = compare_attendance(backend_data, new_attendance)
        generate_report(report_entries)
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        logging.error("Please check the file structures and ensure they contain the expected columns.")
        import traceback
        logging.error(traceback.format_exc())

if __name__ == "__main__":
    main()