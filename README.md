# Attendance Reconciliation Tool

This tool streamlines the process of reconciling employee attendance between a backend system (such as Qandle Dump) and manual attendance records. It compares the data and generates a report highlighting discrepancies for review.

## Prerequisites
- Python 3.7+
- Libraries:
  - pandas
  - openpyxl
  - tqdm
  - gradio
  - schedule

To install the required libraries, run:

```bash
pip install -r requirements.txt
```

## File Structure
Ensure the following structure for the tool to work correctly:

1. Backend Data File:
Store the backend data (e.g., Qandle.xlsx) in the backend directory.

2. New Attendance File:
Pass the new attendance file as an argument when running the tool.

## Usage
### Command-Line Execution
Run the script using the following command:

```bash
python main.py --file <path_to_new_attendance_file>
```

Example:
```bash
python main.py --file "new_attendance.xlsx"
```

## Input and Output
### Backend Data:
The backend file should be stored as backend/Qandle.xlsx (or other supported formats).
The sheet name in the backend file should be Qandle.

### New Attendance Data:
The new attendance file must contain a sheet named Attn. If absent, the tool will default to the first sheet.

### Generated Report:
After processing, the tool will generate an Excel report (attendance_discrepancy_report.xlsx), summarizing mismatches between the two datasets.

### Report Columns
| Column   | Description                                |
|----------|--------------------------------------------|
| Emp ID   | Employee ID                                |
| Emp Name | Employee Name                              |
| Date     | Date of the attendance entry               |
| Manual   | Value from the manual attendance file      |
| Qandle   | Value from the Qandle file                 |
| Mismatch | Indicates if a mismatch was found          |

## Troubleshooting
- Unsupported File Format: Ensure files are in .xls, .xlsx, .csv, .xlsm, or .xlsb format.
- Missing Columns: If essential columns like Employee Code or Employee Name are absent, the tool will prompt for the correct column names.
- File Not Found: Ensure the backend file is stored in the backend directory, and the new attendance file path is accurate.