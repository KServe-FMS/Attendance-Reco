import pandas as pd
import os
import argparse
from datetime import datetime

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
                    # Try parsing as 'dd-mm-yyyy'
                    date = pd.to_datetime(col, format='%d-%m-%Y')
                    new_columns.append(date.strftime('Status (%d-%b-%y)'))
                except ValueError:
                    try:
                        # Try parsing as 'Status (dd-mmm-yy)'
                        date_str = col.split('(')[1].split(')')[0]
                        date = pd.to_datetime(date_str, format='%d-%b-%y')
                        new_columns.append(date.strftime('Status (%d-%b-%y)'))
                    except (IndexError, ValueError):
                        # If parsing fails, keep the original column name
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
        if file_extension in ['.xls', '.xlsx', '.xlsm', '.xlsb']:
            engine = 'pyxlsb' if file_extension == '.xlsb' else None
            df = pd.read_excel(file_path, sheet_name=sheet_name, engine=engine)
        elif file_extension == '.csv':
            df = pd.read_csv(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_extension}")
        
        print(f"Columns in the loaded dataframe: {df.columns.tolist()}")
        return df
    except Exception as e:
        print(f"Error loading file {file_path}: {str(e)}")
        raise

def process_dataframe(df, id_col='Employee Code', name_col='Employee Name'):
    df = standardize_column_names(df)
    
    if id_col not in df.columns:
        print(f"Warning: '{id_col}' column not found. Available columns: {df.columns.tolist()}")
        id_col = input("Please enter the correct column name for Employee ID: ").strip()
        
        # Check if the user input is valid
        while id_col not in df.columns:
            print(f"Error: '{id_col}' is not a valid column name.")
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
            print(f"Warning: Unable to parse date from column '{col}'. Keeping original name.")
    
    # Convert date columns to string to maintain consistency
    for col in df.columns:
        if col != name_col:
            df[col] = df[col].astype(str)
    
    return df

def load_backend_data(file_path, sheet_name):
    df = load_file(file_path, sheet_name)
    print(f"Loaded backend data from {file_path}, sheet: {sheet_name}")
    return process_dataframe(df)

def load_new_attendance(file_path):
    try:
        df = load_file(file_path, "Attn")
        print(f"Loaded new attendance data from {file_path}, sheet: Attn")
    except ValueError:
        print(f"Warning: 'Attn' sheet not found in {file_path}. Using the first sheet.")
        df = load_file(file_path)
        print(f"Loaded new attendance data from {file_path}, first sheet")
    
    return process_dataframe(df)

def compare_attendance(backend_data, new_attendance):
    report_entries = []
    
    # Iterate through each employee in the new attendance data
    for emp_id in new_attendance.index:
        if emp_id in backend_data.index:
            emp_name_attn = new_attendance.at[emp_id, 'Employee Name']
            emp_name_qandle = backend_data.at[emp_id, 'Employee Name']
            
            # Removed the warning for different names
            emp_name = emp_name_attn  # Prefer Attn's name
            
            for date in new_attendance.columns:
                if date == 'Employee Name':
                    continue  # Skip the Employee Name column
                
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
        print("No data to generate report.")
        return

    report_df = pd.DataFrame(report_entries)
    
    # Reorder columns to match the requested format
    report_df = report_df[["Emp ID", "Emp Name", "Date", "Attn", "Qandle", "Mismatch"]]
    
    report_file = "attendance_discrepancy_report.xlsx"
    report_df.to_excel(report_file, index=False)
    print(f"Discrepancy report generated: {report_file}")
    total_mismatches = report_df[report_df["Mismatch"] == "Yes"].shape[0]
    print(f"Total mismatches found: {total_mismatches}")

def main():
    parser = argparse.ArgumentParser(description="Attendance Reconciliation Tool")
    parser.add_argument("--file", help="Path to the new attendance file")
    args = parser.parse_args()

    backend_file = next((os.path.join("backend", f"Qandle{ext}") 
                         for ext in ['.xlsx', '.xlsm', '.xls', '.csv', '.xlsb'] 
                         if os.path.exists(os.path.join("backend", f"Qandle{ext}"))), 
                        None)
    
    if not backend_file:
        raise FileNotFoundError("Backend file not found in the 'backend' directory.")
    
    print(f"Found backend file: {backend_file}")
    
    if args.file:
        new_attendance_file = args.file
    else:
        new_attendance_file = input("Enter the path to the new attendance file: ").strip()
    
    if not os.path.exists(new_attendance_file):
        raise FileNotFoundError(f"New attendance file not found: {new_attendance_file}")

    try:
        backend_data = load_backend_data(backend_file, sheet_name="Qandle")
        new_attendance = load_new_attendance(new_attendance_file)
        report_entries = compare_attendance(backend_data, new_attendance)
        generate_report(report_entries)
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        print("Please check the file structures and ensure they contain the expected columns.")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()