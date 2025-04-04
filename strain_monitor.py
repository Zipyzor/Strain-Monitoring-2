import os
import time
import http.client
import struct
from dotenv import load_dotenv
from datetime import datetime
import pytz
import smtplib
from email.message import EmailMessage

# Load environment variables
load_dotenv()

# SensorCloud authentication details
DEVICE_ID = os.getenv("DEVICE_ID")
AUTH_KEY = os.getenv("AUTH_KEY")

# Sensor and renamed channels
SENSOR_NAME = "44936"
CHANNELS = {
    "ch1": "NCR Axial",
    "ch2": "NCR Bending",
    "ch3": "Control Axial"
}

# API Authentication Server
AUTH_SERVER = "sensorcloud.microstrain.com"
PACIFIC_TZ = pytz.timezone("America/Los_Angeles")


def authenticate_key(device_id, key):
    """Authenticate with SensorCloud and get the server and auth_token for API requests."""
    conn = http.client.HTTPSConnection(AUTH_SERVER)
    headers = {"Accept": "application/xdr"}
    url = f"/SensorCloud/devices/{device_id}/authenticate/?version=1&key={key}"

    conn.request('GET', url=url, headers=headers)
    response = conn.getresponse()

    if response.status == http.client.OK:
        data = response.read()

        if len(data) < 8:
            return None, None  # Invalid response

        # Extract auth token length and value
        auth_token_length = struct.unpack("!I", data[:4])[0]
        auth_token = data[4:4 + auth_token_length].decode('utf-8').strip('\x00')

        # Find correct server offset dynamically
        server_offset = 4 + auth_token_length  
        while server_offset < len(data) - 4:
            server_length = struct.unpack("!I", data[server_offset:server_offset + 4])[0]
            if 1 <= server_length <= 100:  # Ensure valid length
                break
            server_offset += 1  

        # Extract server name
        server = data[server_offset + 4:server_offset + 4 + server_length].decode('utf-8').strip('\x00')

        return server, auth_token
    else:
        return None, None  # Authentication failed


def format_timestamp(nanoseconds):
    """Convert nanoseconds timestamp to Pacific Time in 12-hour AM/PM format."""
    timestamp_seconds = nanoseconds / 1e9
    utc_time = datetime.utcfromtimestamp(timestamp_seconds).replace(tzinfo=pytz.utc)
    return utc_time.astimezone(PACIFIC_TZ).strftime('%m/%d/%Y %I:%M:%S %p')


def download_data_range(server, auth_token, device_id, sensor_name, channel_name, start_time_ns, end_time_ns):
    """Download all data for a given sensor channel within a specific time range."""
    conn = http.client.HTTPSConnection(server)
    url = f"/SensorCloud/devices/{device_id}/sensors/{sensor_name}/channels/{channel_name}/streams/timeseries/data/"
    url += f"?version=1&auth_token={auth_token}&starttime={start_time_ns}&endtime={end_time_ns}"
    headers = {"Accept": "application/xdr"}

    conn.request("GET", url=url, headers=headers)
    response = conn.getresponse()

    if response.status == http.client.OK:
        data = response.read()
        data_points = []

        while data:
            timestamp = struct.unpack_from("!Q", data, 0)[0]
            value = struct.unpack_from("!f", data, 8)[0]
            data_points.append((timestamp, value))
            data = data[12:]  # Move to next entry

        return data_points
    return []  # Return empty list if no data


def get_peak_values(server, auth_token):
    """Fetch the last 2 minutes of data for each channel and find the peak value."""
    now_ns = int(time.time() * 1e9)
    start_ns = now_ns - int(2 * 60 * 1e9)

    peak_values = {}
    for channel in CHANNELS:
        data = download_data_range(server, auth_token, DEVICE_ID, SENSOR_NAME, channel, start_ns, now_ns)
        peak_values[channel] = max((value for _, value in data), default=None)

    return peak_values


def calculate_peak_difference(server, auth_token):
    """Calculate the peak (NCR Axial - Control Axial) difference over the last 2 minutes, allowing closest matches."""
    now_ns = int(time.time() * 1e9)
    start_ns = now_ns - int(2 * 60 * 1e9)

    ch1_data = download_data_range(server, auth_token, DEVICE_ID, SENSOR_NAME, "ch1", start_ns, now_ns)
    ch3_data = download_data_range(server, auth_token, DEVICE_ID, SENSOR_NAME, "ch3", start_ns, now_ns)

    if not ch1_data:
        print("⚠️ Warning: No recent data for NCR Axial (ch1)")
        return None
    if not ch3_data:
        print("⚠️ Warning: No recent data for Control Axial (ch3)")
        return None

    # Convert lists to dictionaries for fast lookup
    ch1_dict = {ts: val for ts, val in ch1_data}
    ch3_dict = {ts: val for ts, val in ch3_data}

    # Get all timestamps sorted
    ch1_timestamps = sorted(ch1_dict.keys())
    ch3_timestamps = sorted(ch3_dict.keys())

    # Match timestamps as closely as possible
    matched_differences = []
    ch3_index = 0
    mismatch_reports = []  # Store time mismatches

    for ts in ch1_timestamps:
        # Move ch3_index forward until we find the closest match
        while ch3_index < len(ch3_timestamps) - 1 and abs(ch3_timestamps[ch3_index + 1] - ts) < abs(ch3_timestamps[ch3_index] - ts):
            ch3_index += 1

        closest_ts = ch3_timestamps[ch3_index]
        time_diff_sec = abs(closest_ts - ts) / 1e9  # Convert nanoseconds to seconds

        if time_diff_sec > 0:  # Only report mismatches
            mismatch_reports.append(f"⏳ Time mismatch: {time_diff_sec:.3f} sec between NCR Axial ({ts}) and Control Axial ({closest_ts})")

        if time_diff_sec <= 1.0:  # Allow up to 1 second mismatch
            matched_differences.append(ch1_dict[ts] - ch3_dict[closest_ts])

    # Print mismatches only if they exist
    if mismatch_reports:
        print("\n".join(mismatch_reports))

    # Return peak difference
    return max(matched_differences, default=None)


def send_email_alert(value):
    print("📧 Preparing to send email alert...")
    msg = EmailMessage()
    msg["Subject"] = "🚨 Strain Monitor Alert: Strain Difference Exceeded"
    msg["From"] = os.getenv("EMAIL_SENDER")
    msg["To"] = os.getenv("EMAIL_RECIPIENTS")
    msg.set_content(f"THIS IS A TEST OF THE EMERGENCY BROADCAST SYSTEM: The peak strain difference exceeded the threshold for NCR1588719800.\n\nValue: {value:.2f}")

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
            smtp.starttls()
            smtp.login(msg["From"], os.getenv("EMAIL_PASSWORD"))
            smtp.send_message(msg)
            print("✅ Email alert sent.")
    except Exception as e:
        print(f"❌ Failed to send email alert: {e}")




def main():
    """Main function to authenticate and fetch live strain data every 2 minutes."""
    server, auth_token = authenticate_key(DEVICE_ID, AUTH_KEY)
    if not server or not auth_token:
        print("Exiting due to authentication failure.")
        return

    THRESHOLD = 274  # Add peak threshold here
    alert_sent = False  # Flag to avoid spamming email

    try:
        while True:
            print("\nFetching live data...")

            now_ns = int(time.time() * 1e9)
            start_ns = now_ns - int(2 * 60 * 1e9)

            for channel, name in CHANNELS.items():
                data = download_data_range(server, auth_token, DEVICE_ID, SENSOR_NAME, channel, start_ns, now_ns)
                latest_value = data[-1][1] if data else None
                print(f"{name}: {latest_value if latest_value is not None else 'No data available'}")

            print("\nAnalyzing last 2 minutes of data...")
            peak_values = get_peak_values(server, auth_token)
            for channel, name in CHANNELS.items():
                peak = peak_values[channel]
                print(f"Peak Value for {name} (last 2 min): {peak if peak is not None else 'No data'}")

            peak_difference = calculate_peak_difference(server, auth_token)

            if peak_difference is not None:
                abs_diff = abs(peak_difference)
                print(f"Peak (NCR Axial - Control Axial) Difference (last 2 min): {peak_difference:.2f}")
                if abs_diff > THRESHOLD:
                    print(f"🚨 Threshold breached! Absolute difference = {abs_diff:.2f} (Threshold = {THRESHOLD})")
                    print(f"🔍 alert_sent is {alert_sent}")
                    if not alert_sent:
                        send_email_alert(peak_difference)
                        alert_sent = True
                else:
                    print(f"✅ Difference within threshold. (Max = {abs_diff:.2f})")
            else:
                print("Peak (NCR Axial - Control Axial) Difference (last 2 min): No data")

            print("\nWaiting for next data update (2 minutes)...")
            time.sleep(120)

    except KeyboardInterrupt:
        print("\nStopping live data retrieval.")


if __name__ == "__main__":
    main()
