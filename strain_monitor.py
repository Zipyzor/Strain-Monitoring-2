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

# Runtime summary trackers
threshold_breached = False
max_divergence = 0
loop_gap_count = 0
previous_timestamps = None

EXPECTED_HZ = 32
EXPECTED_WINDOW_SECONDS = 120
EXPECTED_SAMPLE_COUNT = EXPECTED_HZ * EXPECTED_WINDOW_SECONDS


def authenticate_key(device_id, key):
    conn = http.client.HTTPSConnection(AUTH_SERVER)
    headers = {"Accept": "application/xdr"}
    url = f"/SensorCloud/devices/{device_id}/authenticate/?version=1&key={key}"
    conn.request('GET', url=url, headers=headers)
    response = conn.getresponse()

    if response.status == http.client.OK:
        data = response.read()
        if len(data) < 8:
            return None, None
        auth_token_length = struct.unpack("!I", data[:4])[0]
        auth_token = data[4:4 + auth_token_length].decode('utf-8').strip('\x00')
        server_offset = 4 + auth_token_length
        while server_offset < len(data) - 4:
            server_length = struct.unpack("!I", data[server_offset:server_offset + 4])[0]
            if 1 <= server_length <= 100:
                break
            server_offset += 1
        server = data[server_offset + 4:server_offset + 4 + server_length].decode('utf-8').strip('\x00')
        return server, auth_token
    else:
        return None, None


def download_data_range(server, auth_token, device_id, sensor_name, channel_name, start_time_ns, end_time_ns):
    conn = http.client.HTTPSConnection(server)
    url = f"/SensorCloud/devices/{device_id}/sensors/{sensor_name}/channels/{channel_name}/streams/timeseries/data/?version=1&auth_token={auth_token}&starttime={start_time_ns}&endtime={end_time_ns}"
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
            data = data[12:]
        return data_points
    return []


def send_email_alert(value):
    msg = EmailMessage()
    msg["Subject"] = "\U0001F6A8 Strain Monitor Alert: Strain Difference Exceeded"
    msg["From"] = os.getenv("EMAIL_SENDER")
    msg["To"] = os.getenv("EMAIL_RECIPIENTS")
    msg.set_content(f"The peak strain difference exceeded the threshold.\n\nValue: {value:.2f}")

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
            smtp.starttls()
            smtp.login(msg["From"], os.getenv("EMAIL_PASSWORD"))
            smtp.send_message(msg)
            print("✅ Email alert sent.")
    except Exception as e:
        print(f"❌ Failed to send email alert: {e}")


def calculate_peak_difference_with_adaptive_window(server, auth_token):
    now_ns = int(time.time() * 1e9)
    start_ns = now_ns - int(10 * 60 * 1e9)

    ch1_data = download_data_range(server, auth_token, DEVICE_ID, SENSOR_NAME, "ch1", start_ns, now_ns)
    ch3_data = download_data_range(server, auth_token, DEVICE_ID, SENSOR_NAME, "ch3", start_ns, now_ns)

    if not ch1_data or not ch3_data:
        return None, None, 0.0, set()

    ch1_dict = dict(ch1_data)
    ch3_dict = dict(ch3_data)
    common_timestamps = sorted(set(ch1_dict.keys()) & set(ch3_dict.keys()))

    if not common_timestamps:
        return None, None, 0.0, set()

    matched_differences = [(ts, ch1_dict[ts] - ch3_dict[ts]) for ts in common_timestamps]
    peak_ts, peak_val = max(matched_differences, key=lambda x: abs(x[1]))

    latest_analyzed_ts = common_timestamps[-1]
    delay_to_latest_data = (now_ns - latest_analyzed_ts) / 1e9

    duration_sec = (max(common_timestamps) - min(common_timestamps)) / 1e9
    return peak_val, delay_to_latest_data, duration_sec, set(common_timestamps)


def main():
    global threshold_breached, max_divergence, loop_gap_count, previous_timestamps

    server, auth_token = authenticate_key(DEVICE_ID, AUTH_KEY)
    if not server or not auth_token:
        print("Exiting due to authentication failure.")
        return

    THRESHOLD = 274
    alert_sent = False

    try:
        while True:
            peak_difference, delay, duration_sec, current_timestamps = calculate_peak_difference_with_adaptive_window(server, auth_token)

            if peak_difference is not None:
                abs_diff = abs(peak_difference)
                overlap_status = ""

                if previous_timestamps is not None:
                    if previous_timestamps & current_timestamps:
                        overlap_status = "✅ No data gap detected"
                    else:
                        overlap_status = "⚠️ Timestamp gap detected"
                        loop_gap_count += 1

                previous_timestamps = current_timestamps

                if abs_diff > THRESHOLD:
                    print(f"\U0001F6A8 Divergence ABOVE threshold: {abs_diff:.2f} (Delay to latest data: {delay:.1f} sec, Analyzed Window: Last {duration_sec / 60:.2f} min) {overlap_status}")
                    if not alert_sent:
                        send_email_alert(peak_difference)
                        alert_sent = True
                    threshold_breached = True
                else:
                    print(f"✅ Divergence OK: {abs_diff:.2f} (Delay: {delay:.1f} sec, Analyzed Window: Last {duration_sec / 60:.2f} min) {overlap_status}")

                if abs_diff > max_divergence:
                    max_divergence = abs_diff
            else:
                print("⚠️ No divergence data available.")

            time.sleep(120)

    except KeyboardInterrupt:
        print("\n📋 Run Summary:")
        print(f" - Threshold breached: {'Yes' if threshold_breached else 'No'}")
        print(f" - Maximum recorded divergence: {max_divergence:.2f}")
        print(f" - Timestamp gaps detected: {loop_gap_count}")
        print("Stopping live data retrieval.")

if __name__ == "__main__":
    main()