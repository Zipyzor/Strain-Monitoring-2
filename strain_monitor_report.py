import os
import pandas as pd
import matplotlib.pyplot as plt
from fpdf import FPDF
from datetime import datetime
import pytz
import time

# Define folders
DATA_DIR = "data"
REPORTS_DIR = "reports"
KEY_FILE = "NCR-Control-Elements.csv"
PACIFIC_TZ = pytz.timezone("America/Los_Angeles")

# Ensure report directory exists
os.makedirs(REPORTS_DIR, exist_ok=True)

# Load key file
def load_key_file():
    key_path = os.path.join(DATA_DIR, KEY_FILE)
    key_df = pd.read_csv(key_path)
    key_df.columns = key_df.columns.str.strip().str.lower()
    key_df['control'] = key_df['control'].astype(str).str.strip().str.lower().map({'true': True, 'false': False})
    return key_df

# Select file from data folder
def select_data_file():
    files = [f for f in os.listdir(DATA_DIR) if f.endswith(".csv") and f != KEY_FILE]
    print("Available data files:")
    for idx, file in enumerate(files):
        print(f"[{idx+1}] {file}")
    choice = int(input("Select a file number: ")) - 1
    return os.path.join(DATA_DIR, files[choice])

# Load data skipping metadata
def load_data(filepath, ncr_channel, ctrl_channel):
    dtype_spec = {
        'Time': 'int64',
        ncr_channel: 'float32',
        ctrl_channel: 'float32',
    }
    df = pd.read_csv(filepath, skiprows=18, usecols=['Time', ncr_channel, ctrl_channel], dtype=dtype_spec)
    df.columns = df.columns.str.strip()
    df = df.dropna(subset=['Time'])
    df = df[df['Time'] > 0]
    df['Time'] = pd.to_datetime(df['Time'], unit='ns', utc=True)
    return df

# Match data channels to key file
def identify_channels(key_df):
    ncr_row = key_df[key_df['control'] == False].iloc[0]
    ctrl_row = key_df[key_df['control'] == True].iloc[0]
    return (ncr_row, ctrl_row)

# Generate and save plots
def create_plots(df, ncr, ctrl, filename):
    from matplotlib.dates import DateFormatter
    timestamps = df['Time'].dt.tz_convert(PACIFIC_TZ)
    ncr_channel = ncr['channel']
    ctrl_channel = ctrl['channel']
    if ncr_channel not in df.columns or ctrl_channel not in df.columns:
        return None, None, pd.Series(dtype='float64')
    if df[ncr_channel].isnull().all() or df[ctrl_channel].isnull().all():
        return None, None, pd.Series(dtype='float64')

    divergence = df[ncr_channel] - df[ctrl_channel]
    peak_idx = divergence.abs().idxmax()
    peak_time = df.loc[peak_idx, 'Time'].tz_convert(PACIFIC_TZ)

    ncr_label = f"NCR ({ncr['element id']})"
    ctrl_label = f"Control ({ctrl['element id']})"
    combined_plot_path = os.path.join(REPORTS_DIR, filename + "_combined.png")
    fig, axs = plt.subplots(2, 1, figsize=(8, 8), dpi=300)
    axs[0].plot(timestamps, df[ncr_channel], label=ncr_label)
    axs[0].plot(timestamps, df[ctrl_channel], label=ctrl_label)
    axs[0].set_title("Strain vs Time")
    axs[0].set_xlabel("Time")
    axs[0].set_ylabel("Strain")
    axs[0].axvline(x=peak_time, color='black', linestyle='--', linewidth=1)
    axs[0].annotate('Peak Divergence', xy=(peak_time, df[ncr_channel].loc[peak_idx]), xytext=(10, 10), textcoords='offset points', arrowprops=dict(arrowstyle='->'), fontsize=8)
    axs[0].legend()
    axs[0].axvline(x=peak_time, color='black', linestyle='--', linewidth=1)
    axs[0].annotate('Peak Divergence', xy=(peak_time, df[ncr_channel].loc[peak_idx]), xytext=(10, 10), textcoords='offset points', arrowprops=dict(arrowstyle='->'), fontsize=8)
    from matplotlib.dates import DateFormatter
    axs[0].xaxis.set_major_formatter(DateFormatter('%H:%M', tz=PACIFIC_TZ))
    axs[0].tick_params(axis='x', rotation=45)
    divergence = df[ncr_channel] - df[ctrl_channel]
    axs[1].plot(timestamps, divergence, label="Divergence (NCR - Control)", color='red')
    peak_idx = divergence.abs().idxmax()
    peak_time = df.loc[peak_idx, 'Time'].tz_convert(PACIFIC_TZ)
    axs[1].axvline(x=peak_time, color='black', linestyle='--', linewidth=1)
    axs[1].annotate('Peak Divergence', xy=(peak_time, divergence.loc[peak_idx]), xytext=(10, 10), textcoords='offset points', arrowprops=dict(arrowstyle='->'), fontsize=8)
    axs[1].set_title("Strain Divergence vs Time")
    axs[1].set_xlabel("Time")
    axs[1].set_ylabel("Strain Divergence")
    axs[1].legend()
    axs[1].xaxis.set_major_formatter(DateFormatter('%H:%M', tz=PACIFIC_TZ))
    axs[1].tick_params(axis='x', rotation=45)
    plt.tight_layout()
    plt.savefig(combined_plot_path, bbox_inches='tight')
    plt.close()
    return combined_plot_path, None, divergence

# Generate PDF Report
def generate_pdf(ncr, ctrl, df, divergence, plots, filename):
    ncr_number = str(ncr.iloc[0]).strip()
    clean_title = f"Strain Monitoring Report for {ncr_number}"
    clean_filename = f"{filename}_report_{ncr_number.replace(' ', '_').replace('/', '-')}.pdf"
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Arial", 'B', 11)
    pdf.cell(0, 5, clean_title, ln=True)
    pdf.set_font("Arial", size=8)
    pdf.cell(0, 4, f"Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", ln=True)
    start_time = df['Time'].min().tz_convert(PACIFIC_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')
    end_time = df['Time'].max().tz_convert(PACIFIC_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')
    pdf.cell(0, 4, f"Data Start Time: {start_time}", ln=True)
    pdf.cell(0, 4, f"Data End Time: {end_time}", ln=True)
    pdf.set_font("Arial", 'B', 10)
    pdf.cell(0, 5, "Summary Statistics", ln=True)
    pdf.set_font("Arial", size=8)
    for beam, label in zip([ncr, ctrl], ["NCR", "Control"]):
        channel = beam['channel']
        if channel in df.columns:
            channel_data = df[channel].dropna()
            if not channel_data.empty:
                pdf.cell(0, 4, f"{label} Beam (Element ID: {beam['element id']})", ln=True)
                pdf.cell(0, 4, f" - Type: {beam['type']}", ln=True)
                pdf.cell(0, 4, f" - Peak: {channel_data.max():.2f}, Min: {channel_data.min():.2f}, Mean: {channel_data.mean():.2f}", ln=True)
            else:
                pdf.cell(0, 4, f"{label} Beam data missing or empty.", ln=True)
        else:
            pdf.cell(0, 4, f"{label} Beam channel '{channel}' not found in data.", ln=True)
    if not divergence.empty:
        peak_div = divergence.abs().max()
        peak_idx = divergence.abs().idxmax()
        peak_time = df.loc[peak_idx, 'Time']
        if peak_time.tzinfo is None:
            peak_time = peak_time.tz_localize('UTC')
        peak_time = peak_time.tz_convert(PACIFIC_TZ).strftime('%Y-%m-%d %H:%M:%S')

        pre_peak = divergence[:peak_idx].dropna()
        post_peak = divergence[peak_idx+1:].dropna()
        pre_avg = pre_peak.abs().mean() if not pre_peak.empty else float('nan')
        post_avg = post_peak.abs().mean() if not post_peak.empty else float('nan')
        pdf.cell(0, 4, f"Peak Absolute Divergence: {peak_div:.2f}", ln=True)
        pdf.cell(0, 4, f"Occurred at: {peak_time}", ln=True)
        pdf.cell(0, 4, f"Average Absolute Divergence Before Peak: {pre_avg:.2f}", ln=True)
        pdf.cell(0, 4, f"Average Absolute Divergence After Peak: {post_avg:.2f}", ln=True)
    else:
        pdf.cell(0, 4, "No divergence data available.", ln=True)
    if plots[0] and os.path.exists(plots[0]):
        pdf.ln(1)
        pdf.image(plots[0], x=10, w=190)
    report_file = os.path.join(REPORTS_DIR, clean_filename)
    pdf.output(report_file)
    print(f"\n✅ Report created: {report_file}")

# Main function
def main():
    start = time.time()
    key_df = load_key_file()
    print(f"Loaded key file in {time.time() - start:.2f} sec")

    filepath = select_data_file()
    filename = os.path.splitext(os.path.basename(filepath))[0]

    start = time.time()
    ncr, ctrl = identify_channels(key_df)
    print(f"Identified channels in {time.time() - start:.2f} sec")

    start = time.time()
    df = load_data(filepath, ncr['channel'], ctrl['channel'])
    print(f"Loaded data file in {time.time() - start:.2f} sec")

    start = time.time()
    plots_path, _, divergence = create_plots(df, ncr, ctrl, filename)
    plots = (plots_path, None)
    print(f"Created plots in {time.time() - start:.2f} sec")

    start = time.time()
    generate_pdf(ncr, ctrl, df, divergence, plots, filename)
    print(f"Generated PDF in {time.time() - start:.2f} sec")

if __name__ == "__main__":
    main()
