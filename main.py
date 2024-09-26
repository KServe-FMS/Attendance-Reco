import pandas as pd
import os
import argparse
from datetime import datetime
import logging
from tqdm import tqdm
import io
import gradio as gr
import uuid
import shutil
import schedule
import time
import threading

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

def load_file(file_path=None, file_bytes=None, sheet_name=None):
    """
    Load a file into a pandas DataFrame.
    
    Parameters:
    - file_path: Path to the file.
    - file_bytes: Bytes of the file (not used here).
    - sheet_name: Name of the sheet to load (for Excel files).
    
    Returns:
    - pandas DataFrame or dict of DataFrames
    """
    file_extension = os.path.splitext(file_path)[1].lower() if file_path else ''
    
    try:
        if file_extension in ['.xls', '.xlsx', '.xlsm']:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            if isinstance(df, dict):
                logging.info(f"Loaded Excel file with multiple sheets: {list(df.keys())}")
                return df
            else:
                logging.info(f"Columns in the loaded dataframe: {df.columns.tolist()}")
                return df
        elif file_extension == '.csv':
            df = pd.read_csv(file_path)
            logging.info(f"Columns in the loaded dataframe: {df.columns.tolist()}")
            return df
        else:
            raise ValueError(f"Unsupported file format: {file_extension}")
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

def load_backend_data(file_path=None, file_bytes=None, sheet_name="Qandle"):
    df = load_file(file_path=file_path, file_bytes=file_bytes, sheet_name=sheet_name)
    logging.info(f"Loaded backend data from {file_path if file_path else 'uploaded file'}, sheet: {sheet_name}")
    return process_dataframe(df)

def load_new_attendance(file_path=None, file_bytes=None):
    try:
        logging.info(f"Loading new attendance data from file: {file_path}")
        if file_path:
            loaded_data = load_file(file_path=file_path, sheet_name=None)  # Load all sheets
            if isinstance(loaded_data, dict):
                # If multiple sheets, use the first one or ask user to specify
                sheet_name = next(iter(loaded_data))
                logging.info(f"Multiple sheets found. Using sheet: {sheet_name}")
                df = loaded_data[sheet_name]
            else:
                df = loaded_data
        elif file_bytes:
            df = pd.read_csv(io.StringIO(file_bytes.decode('utf-8')))
        else:
            raise ValueError("Unsupported file type")
        
        logging.info(f"Loaded new attendance data, shape: {df.shape}")
        logging.info(f"Columns in new attendance data: {df.columns.tolist()}")
        
        processed_df = process_dataframe(df)
        logging.info(f"Processed new attendance data, shape: {processed_df.shape}")
        logging.info(f"Columns in processed new attendance data: {processed_df.columns.tolist()}")
        
        return processed_df
    except Exception as e:
        logging.error(f"Error loading new attendance data: {str(e)}")
        logging.error(f"Error details: {type(e).__name__}")
        import traceback
        logging.error(traceback.format_exc())
        raise

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
        return None
    
    report_df = pd.DataFrame(report_entries)
    
    # Reorder columns to match the requested format
    report_df = report_df[["Emp ID", "Emp Name", "Date", "Attn", "Qandle", "Mismatch"]]
    
    # Save the report to a BytesIO object instead of a file
    report_buffer = io.BytesIO()
    report_df.to_excel(report_buffer, index=False)
    report_buffer.seek(0)
    
    logging.info("Discrepancy report generated in memory.")
    total_mismatches = report_df[report_df["Mismatch"] == "Yes"].shape[0]
    logging.info(f"Total mismatches found: {total_mismatches}")
    
    return report_buffer

def generate_unique_id():
    return str(uuid.uuid4())

def save_uploaded_file(uploaded_file_path, unique_id):
    os.makedirs('attn', exist_ok=True)
    file_extension = os.path.splitext(uploaded_file_path)[1]
    new_filename = f"{unique_id}{file_extension}"
    new_path = os.path.join('attn', new_filename)
    shutil.copy(uploaded_file_path, new_path)
    logging.info(f"Saved uploaded file to {new_path}")
    return new_path

def schedule_file_deletion(file_path, delay_hours=1):
    def delete_file():
        try:
            os.remove(file_path)
            logging.info(f"Deleted file: {file_path}")
        except FileNotFoundError:
            logging.warning(f"File not found for deletion: {file_path}")
        except Exception as e:
            logging.error(f"Error deleting file {file_path}: {str(e)}")

    schedule.every(delay_hours).hours.do(delete_file)
    logging.info(f"Scheduled deletion of {file_path} in {delay_hours} hour(s)")

def run_schedule():
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

# Start the scheduling thread
threading.Thread(target=run_schedule, daemon=True).start()

def process_attendance_file(uploaded_file_path):
    """
    Processes the uploaded attendance file and generates a discrepancy report.
    
    Parameters:
    - uploaded_file_path: The path to the uploaded attendance file.
    
    Returns:
    - A tuple containing the path to the discrepancy report file (or None if no discrepancies) and a status message.
    """
    try:
        unique_id = generate_unique_id()
        logging.info(f"Generated unique ID: {unique_id}")
        
        # Save uploaded file with unique ID
        saved_file_path = save_uploaded_file(uploaded_file_path, unique_id)
        logging.info(f"Saved uploaded file to: {saved_file_path}")
        
        # Schedule deletion of the uploaded file after 1 hour
        schedule_file_deletion(saved_file_path, delay_hours=1)
        logging.info(f"Scheduled deletion of uploaded file: {saved_file_path}")
        
        # Load backend data
        backend_dir = "backend"
        backend_file = next((os.path.join(backend_dir, f"Qandle{ext}") 
                             for ext in ['.xlsx', '.xlsm', '.xls', '.csv'] 
                             if os.path.exists(os.path.join(backend_dir, f"Qandle{ext}"))), 
                            None)
        
        if not backend_file:
            logging.error("Backend file not found in the 'backend' directory.")
            return None, "Backend file not found in the 'backend' directory."
        
        logging.info(f"Loading backend data from: {backend_file}")
        backend_data = load_backend_data(file_path=backend_file)
        logging.info(f"Backend data loaded. Shape: {backend_data.shape}")
        
        # Load new attendance data from the saved file
        file_extension = os.path.splitext(saved_file_path)[1].lower()
        logging.info(f"Loading new attendance data from: {saved_file_path}")
        if file_extension in ['.xls', '.xlsx', '.xlsm', '.csv']:
            new_attendance = load_new_attendance(file_path=saved_file_path)
            logging.info(f"New attendance data loaded. Shape: {new_attendance.shape}")
        else:
            return None, f"Unsupported file format: {file_extension}"
        
        # Compare attendance
        logging.info("Comparing attendance data")
        report_entries = compare_attendance(backend_data, new_attendance)
        logging.info(f"Comparison complete. Number of entries: {len(report_entries)}")
        
        # Generate discrepancy report
        logging.info("Generating discrepancy report")
        report_buffer = generate_report(report_entries)
        
        if report_buffer:
            # Save the report with the unique ID
            report_filename = f"discrepancy_report_{unique_id}.xlsx"
            report_path = os.path.join('attn', report_filename)
            with open(report_path, 'wb') as f:
                f.write(report_buffer.getvalue())
            logging.info(f"Discrepancy report saved to {report_path}")
            
            # Schedule deletion of the discrepancy report after 1 hour
            schedule_file_deletion(report_path, delay_hours=1)
            logging.info(f"Scheduled deletion of discrepancy report: {report_path}")
            
            return report_path, f"Processing complete. Discrepancy report generated at: {report_path}"
        else:
            logging.info("No discrepancies found.")
            return None, "No discrepancies found."
    
    except Exception as e:
        logging.error(f"An error occurred during processing: {str(e)}")
        logging.error(f"Error details: {type(e).__name__}")
        import traceback
        logging.error(traceback.format_exc())
        return None, f"An error occurred: {str(e)}"

def create_gradio_interface():
    """
    Creates and launches the Gradio web interface.
    """
    def process_and_return(file):
        if file is None:
            return None, "No file uploaded. Please upload a file."
        
        try:
            # Gradio's File component provides the path to the uploaded file
            uploaded_file_path = file.name if hasattr(file, 'name') else file
            
            logging.info(f"Received uploaded file: {uploaded_file_path}")
            
            report_path, result = process_attendance_file(uploaded_file_path)
            
            if report_path:
                # If a discrepancy report was generated, return it for download
                return gr.update(value=report_path, visible=True), result
            else:
                # If no discrepancies or an error occurred
                return gr.update(visible=False), result
        except Exception as e:
            logging.error(f"An error occurred in Gradio interface: {str(e)}")
            return gr.update(visible=False), f"An error occurred: {str(e)}"
    
    with gr.Blocks() as demo:
        gr.Markdown("# Attendance Reconciliation Tool")
        gr.Markdown(
            """
            Upload the new attendance file, and the tool will process it against the backend data to generate a discrepancy report.
            Supported file formats: `.xlsx`, `.xls`, `.csv`.
            """
        )
        
        with gr.Row():
            upload = gr.File(label="Upload Attendance File", file_types=[".xlsx", ".xls", ".csv"])
        
        with gr.Row():
            process_button = gr.Button("Process Attendance")
        
        with gr.Row():
            download = gr.File(label="Download Discrepancy Report", visible=False)
        
        with gr.Row():
            output_text = gr.Textbox(label="Status", interactive=False, placeholder="Status messages will appear here.")
        
        process_button.click(
            fn=process_and_return,
            inputs=upload,
            outputs=[download, output_text]
        )
    
    demo.launch()

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    parser = argparse.ArgumentParser(description="Attendance Reconciliation Tool")
    parser.add_argument("--file", help="Path to the new attendance file")
    parser.add_argument("--config", help="Path to the configuration file", default="config.ini")
    parser.add_argument("--web", help="Launch Gradio web interface", action="store_true")
    args = parser.parse_args()

    if args.web:
        create_gradio_interface()
        return
    
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
        report_path, result = process_attendance_file(new_attendance_file)
        
        if report_path:
            logging.info(f"Discrepancy report generated: {report_path}")
        else:
            logging.info(result)
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        logging.error("Please check the file structures and ensure they contain the expected columns.")
        import traceback
        logging.error(traceback.format_exc())

if __name__ == "__main__":
    main()