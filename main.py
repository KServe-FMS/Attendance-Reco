import pandas as pd
import os
import argparse
from datetime import datetime
import logging
from tqdm import tqdm
import io
import uuid
import shutil
import schedule
import time
import threading
import gradio as gr

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
    file_extension = os.path.splitext(file_path)[1].lower() if file_path else ''
    
    try:
        if file_extension in ['.xls', '.xlsx', '.xlsm']:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
        elif file_extension == '.csv':
            df = pd.read_csv(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_extension}")
        
        logging.info(f"Loaded file: {file_path}, shape: {df.shape}")
        return df
    except Exception as e:
        logging.error(f"Error loading file {file_path}: {str(e)}")
        raise

def process_dataframe(df):
    df = standardize_column_names(df)
    
    df.columns.values[0] = 'Employee Code'
    df.columns.values[1] = 'Employee Name'
    
    id_col = 'Employee Code'
    name_col = 'Employee Name'
    
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
            file_extension = os.path.splitext(file_path)[1].lower()
            if file_extension in ['.xls', '.xlsx', '.xlsm']:
                xls = pd.ExcelFile(file_path)
                attn_sheets = [sheet for sheet in xls.sheet_names if sheet.lower() == 'attn']
                if attn_sheets:
                    df = pd.read_excel(file_path, sheet_name=attn_sheets[0])
                    logging.info(f"Using '{attn_sheets[0]}' sheet from the Excel file.")
                else:
                    logging.warning("'Attn' or 'attn' sheet not found. Using the first sheet.")
                    df = pd.read_excel(file_path, sheet_name=0)
            elif file_extension == '.csv':
                df = pd.read_csv(file_path)
            else:
                raise ValueError(f"Unsupported file format: {file_extension}")
        elif file_bytes:
            df = pd.read_csv(io.StringIO(file_bytes.decode('utf-8')))
        else:
            raise ValueError("Unsupported file type")
        
        logging.info(f"Loaded new attendance data, shape: {df.shape}")
        
        processed_df = process_dataframe(df)
        logging.info(f"Processed new attendance data, shape: {processed_df.shape}")
        
        return processed_df
    except Exception as e:
        logging.error(f"Error loading new attendance data: {str(e)}")
        raise

def compare_attendance(backend_data, new_attendance):
    report_entries = []
    
    for emp_id in tqdm(new_attendance.index, desc="Processing employees"):
        if emp_id in backend_data.index:
            emp_name = new_attendance.at[emp_id, 'Employee Name']
            
            for date in new_attendance.columns:
                if date == 'Employee Name':
                    continue
                
                uploaded_value = new_attendance.at[emp_id, date]
                qandle_value = backend_data.at[emp_id, date] if date in backend_data.columns else 'N/A'
                
                if (uploaded_value == 'nan' or pd.isna(uploaded_value)) and (qandle_value == 'nan' or pd.isna(qandle_value)):
                    mismatch = 'No'
                    uploaded_display = ''
                    qandle_display = ''
                elif uploaded_value != qandle_value:
                    mismatch = 'Yes'
                    uploaded_display = uploaded_value if uploaded_value != 'nan' else ''
                    qandle_display = qandle_value if qandle_value != 'nan' else ''
                else:
                    mismatch = 'No'
                    uploaded_display = uploaded_value if uploaded_value != 'nan' else ''
                    qandle_display = qandle_value if qandle_value != 'nan' else ''
                
                try:
                    date_obj = pd.to_datetime(date)
                    date_str = date_obj.strftime('%d-%b-%y')
                except:
                    date_str = date
                
                report_entries.append({
                    "Emp ID": emp_id,
                    "Emp Name": emp_name,
                    "Date": date_str,
                    "Manual": uploaded_display,
                    "Qandle": qandle_display,
                    "Mismatch": mismatch
                })
        else:
            emp_name = new_attendance.at[emp_id, 'Employee Name'] if 'Employee Name' in new_attendance.columns else 'N/A'
            for date in new_attendance.columns:
                if date == 'Employee Name':
                    continue
                
                uploaded_value = new_attendance.at[emp_id, date]
                
                try:
                    date_obj = pd.to_datetime(date)
                    date_str = date_obj.strftime('%d-%b-%y')
                except:
                    date_str = date
                
                report_entries.append({
                    "Emp ID": emp_id,
                    "Emp Name": emp_name,
                    "Date": date_str,
                    "Manual": uploaded_value if not (uploaded_value == 'nan' or pd.isna(uploaded_value)) else '',
                    "Qandle": 'Data Not Found in Qandle',
                    "Mismatch": 'Yes'
                })
    
    return report_entries

def generate_report(report_entries):
    if not report_entries:
        logging.info("No data to generate report.")
        return None
    
    report_df = pd.DataFrame(report_entries)
    report_df = report_df[["Emp ID", "Emp Name", "Date", "Manual", "Qandle", "Mismatch"]]
    
    report_buffer = io.BytesIO()
    report_df.to_excel(report_buffer, index=False)
    report_buffer.seek(0)
    
    logging.info("Discrepancy report generated in memory.")
    total_mismatches = report_df[report_df["Mismatch"] == "Yes"].shape[0]
    logging.info(f"Total mismatches found: {total_mismatches}")
    
    return report_buffer

def generate_unique_id():
    return str(uuid.uuid4())

def save_uploaded_file(uploaded_file_path, unique_id, output_folder='uploads'):
    os.makedirs(output_folder, exist_ok=True)
    file_extension = os.path.splitext(uploaded_file_path)[1]
    new_filename = f"{unique_id}{file_extension}"
    new_path = os.path.join(output_folder, new_filename)
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
        time.sleep(60)

threading.Thread(target=run_schedule, daemon=True).start()

def process_attendance_file(uploaded_file_path, output_folder='reports'):
    try:
        unique_id = generate_unique_id()
        logging.info(f"Generated unique ID: {unique_id}")
        
        saved_file_path = save_uploaded_file(uploaded_file_path, unique_id)
        logging.info(f"Saved uploaded file to: {saved_file_path}")
        
        schedule_file_deletion(saved_file_path, delay_hours=1)
        logging.info(f"Scheduled deletion of uploaded file: {saved_file_path}")
        
        backend_dir = "backend"
        backend_file = next((os.path.join(backend_dir, f"Qandle{ext}") 
                             for ext in ['.xlsx', '.xlsm', '.xls', '.csv'] 
                             if os.path.exists(os.path.join(backend_dir, f"Qandle{ext}"))), 
                            None)
        
        if not backend_file:
            logging.error("Qandle file not found in the 'backend' directory.")
            return None, "Qandle file not found in the 'backend' directory."
        
        logging.info(f"Loading Qandle data from: {backend_file}")
        backend_data = load_backend_data(file_path=backend_file)
        logging.info(f"Qandle data loaded. Shape: {backend_data.shape}")
        
        file_extension = os.path.splitext(saved_file_path)[1].lower()
        logging.info(f"Loading new attendance data from: {saved_file_path}")
        if file_extension in ['.xls', '.xlsx', '.xlsm', '.csv']:
            new_attendance = load_new_attendance(file_path=saved_file_path)
            logging.info(f"New attendance data loaded. Shape: {new_attendance.shape}")
        else:
            return None, f"Unsupported file format: {file_extension}"
        
        logging.info("Comparing attendance data")
        report_entries = compare_attendance(backend_data, new_attendance)
        logging.info(f"Comparison complete. Number of entries: {len(report_entries)}")
        
        logging.info("Generating discrepancy report")
        report_buffer = generate_report(report_entries)
        
        if report_buffer:
            os.makedirs(output_folder, exist_ok=True)
            report_filename = f"{os.path.splitext(os.path.basename(uploaded_file_path))[0]}_discrepancy_report_{unique_id}.xlsx"
            report_path = os.path.join(output_folder, report_filename)
            
            with open(report_path, 'wb') as f:
                f.write(report_buffer.getvalue())
            logging.info(f"Discrepancy report saved to {report_path}")
            
            schedule_file_deletion(report_path, delay_hours=1)
            logging.info(f"Scheduled deletion of discrepancy report: {report_path}")
            
            return report_path, f"Processing complete. Discrepancy report generated at: {report_path}"
        else:
            logging.info("No discrepancies found.")
            return None, "No discrepancies found."
    
    except Exception as e:
        logging.error(f"An error occurred during processing: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return None, f"An error occurred: {str(e)}"

def create_gradio_interface():
    def process_and_return(files):
        if not files:
            return None, "No files uploaded. Please upload at least one file."
        
        results = []
        for file in files:
            try:
                uploaded_file_path = file.name if hasattr(file, 'name') else file
                
                logging.info(f"Received uploaded file: {uploaded_file_path}")
                
                report_path, result = process_attendance_file(uploaded_file_path)
                
                if report_path:
                    results.append((report_path, result))
                else:
                    results.append((None, result))
            except Exception as e:
                logging.error(f"An error occurred in Gradio interface: {str(e)}")
                results.append((None, f"An error occurred: {str(e)}"))
        
        if any(report_path for report_path, _ in results):
            report_paths = [report_path for report_path, _ in results if report_path]
            return gr.update(value=report_paths, visible=True), "\n".join(result for _, result in results)
        else:
            return gr.update(visible=False), "\n".join(result for _, result in results)

    def get_template_file():
        template_path = "templates/Attendance.xlsx"
        if os.path.exists(template_path):
            return template_path
        else:
            return None

    with gr.Blocks() as demo:
        gr.Markdown("# Attendance Reconciliation Tool")
        gr.Markdown(
            """
            Upload one or more attendance files, and the tool will process them against the backend data to generate discrepancy reports.
            Supported file formats: `.xlsx`, `.xls`, `.csv`.
            """
        )
        
        with gr.Row():
            upload = gr.File(label="Upload Attendance Files", file_types=[".xlsx", ".xls", ".csv"], file_count="multiple")
        
        with gr.Row():
            process_button = gr.Button("Process Attendance")
            template_button = gr.Button("Download Template")
        
        with gr.Row():
            download = gr.File(label="Download Discrepancy Reports", visible=False, file_count="multiple")
            template_download = gr.File(label="Template File", visible=False)
        
        with gr.Row():
            output_text = gr.Textbox(label="Status", interactive=False, placeholder="Status messages will appear here.")
        
        process_button.click(
            process_and_return,
            inputs=[upload],
            outputs=[download, output_text],
            api_name="process_attendance",
            concurrency_limit=5
        )

        template_button.click(
            get_template_file,
            inputs=[],
            outputs=[template_download],
            api_name="get_template"
        )
    
    demo.queue()
    demo.launch(server_name="0.0.0.0", server_port=7860, share=True)

def main():
    parser = argparse.ArgumentParser(description="Attendance Reconciliation Tool")
    parser.add_argument("--file", help="Path to the attendance file")
    parser.add_argument("--folder", help="Path to the folder containing attendance files")
    parser.add_argument("--web", action="store_true", help="Launch the Web Interface")
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    
    if not any(vars(args).values()):
        print("Welcome to the Attendance Reconciliation Tool!")
        print("Please choose an option:")
        print("1. Process an attendance file")
        print("2. Process an attendance folder")
        print("3. Launch Web Interface")
        
        choice = input("Enter your choice (1-3): ").strip()
        
        if choice == "1":
            file_path = input("Enter the path to the attendance file: ").strip()
            args.file = file_path
        elif choice == "2":
            folder_path = input("Enter the path to the folder containing attendance files: ").strip()
            args.folder = folder_path
        elif choice == "3":
            args.web = True
        else:
            print("Invalid choice. Exiting.")
            return
    
    if args.web:
        logging.getLogger("gradio").setLevel(logging.WARNING)
        create_gradio_interface()
    elif args.folder:
        input_folder = args.folder
        if not os.path.exists(input_folder):
            logging.error(f"Input folder not found: {input_folder}")
            raise FileNotFoundError(f"Input folder not found: {input_folder}")
        
        logging.info(f"Processing files in folder: {input_folder}")
        
        for filename in os.listdir(input_folder):
            if filename.lower().endswith(('.xlsx', '.xls', '.csv')):
                file_path = os.path.join(input_folder, filename)
                logging.info(f"Processing file: {file_path}")
                
                try:
                    report_path, result = process_attendance_file(file_path, output_folder=input_folder)
                    
                    if report_path:
                        logging.info(f"Discrepancy report generated: {report_path}")
                    else:
                        logging.info(result)
                except Exception as e:
                    logging.error(f"An error occurred while processing {file_path}: {str(e)}")
                    logging.error("Please check the file structure and ensure it contains the expected columns.")
                    import traceback
                    logging.error(traceback.format_exc())
    
    elif args.file:
        uploaded_file_path = args.file
        if not os.path.exists(uploaded_file_path):
            logging.error(f"File not found: {uploaded_file_path}")
            raise FileNotFoundError(f"File not found: {uploaded_file_path}")
        
        logging.info(f"Processing file: {uploaded_file_path}")
        
        try:
            report_path, result = process_attendance_file(uploaded_file_path)
            
            if report_path:
                logging.info(f"Discrepancy report generated: {report_path}")
            else:
                logging.info(result)
        except Exception as e:
            logging.error(f"An error occurred while processing {uploaded_file_path}: {str(e)}")
            logging.error("Please check the file structure and ensure it contains the expected columns.")
            import traceback
            logging.error(traceback.format_exc())
    
    else:
        logging.error("Please provide either --file or --folder or --web argument.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"An error occurred during processing: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
    exit(1)