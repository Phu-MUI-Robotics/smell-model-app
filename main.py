# 1. Import
from influxdb import InfluxDBClient
import streamlit as st
from collections import Counter
from datetime import datetime, time, timedelta
import pandas as pd
import io
import numpy as np
import processDataset
import zipfile
from dotenv import load_dotenv
import os
import pytz

# Load environment variables from .env file
load_dotenv()

# InfluxDB connection settings
host = os.getenv("INFLUXDB_HOST") or "localhost"
port_str = os.getenv("INFLUXDB_PORT")
port = int(port_str) if port_str is not None else 8086  # Default to 8086 if not set
username = os.getenv("INFLUXDB_USER") or ""
password = os.getenv("INFLUXDB_PASS") or ""
database = os.getenv("INFLUXDB_DB") or ""

# Create InfluxDB client
client = InfluxDBClient(
    host=host,
    port=port,
    username=username,
    password=password,
    database=database
)

#---------------------------------------------------------------------------------------

# 2. Functions
def connect_influxdb_v1():
    try:
        client = InfluxDBClient(
            host=os.getenv("INFLUXDB_HOST") or "",
            port=int(os.getenv("INFLUXDB_PORT") or 8086),
            username=os.getenv("INFLUXDB_USER") or "",
            password=os.getenv("INFLUXDB_PASS") or "",
            database=os.getenv("INFLUXDB_DB") or ""
        )
        client.get_list_database()
        print("[DEBUG] Connected to InfluxDB successfully.")
        return client
    except Exception as e:
        print(f"[ERROR] Failed to connect to InfluxDB: {e}")
        return None

def get_measurements(client):
    try:
        result = client.query("SHOW MEASUREMENTS")
        measurements = [m['name'] for m in result.get_points()]
        return measurements
    except Exception as e:
        print(f"[ERROR] Failed to query measurements: {e}")
        return []

# ฟังก์ชันดึง Serial No. จาก measurement ที่เลือก
def get_serial_numbers(client, measurement):
    try:
        query = f'SELECT LAST("sn") AS "sn" FROM "{measurement}" WHERE time > now() - 1d GROUP BY "sName"'
        result = client.query(query)
        serials = []
        for serie in result.raw.get('series', []):
            sn = serie['values'][0][1] if serie['values'] and len(serie['values'][0]) > 1 else None
            if sn:
                serials.append(sn)
        return serials
    except Exception as e:
        print(f"[ERROR] Failed to query serial numbers: {e}")
        return []

#---------------------------------------------------------------------------------------

# 3. UI


client = connect_influxdb_v1()
if client:
    measurements = get_measurements(client)
else:
    measurements = []


st.title("Smell Model Mini-App")

# --- Date & Time Picker ---
col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("วันที่เริ่มต้น", value=datetime.now().date() - timedelta(days=1))
    start_time = st.time_input("เวลาเริ่มต้น", value=time(0, 0))
with col2:
    end_date = st.date_input("วันที่สิ้นสุด", value=datetime.now().date())
    end_time = st.time_input("เวลาสิ้นสุด", value=time(23, 59))

# รวมวันและเวลาเป็น datetime (ใช้ Bangkok timezone)
bangkok_tz = pytz.timezone('Asia/Bangkok')
start_dt = bangkok_tz.localize(datetime.combine(start_date, start_time))
end_dt = bangkok_tz.localize(datetime.combine(end_date, end_time))

# แปลงเป็น unix timestamp (วินาที) - จะเป็น UTC timestamp
start_unix = int(start_dt.timestamp())
end_unix = int(end_dt.timestamp())

st.write(f"Unix timestamp เริ่มต้น : {start_unix}")
st.write(f"Unix timestamp สิ้นสุด : {end_unix}")

if not measurements:
    measurements = ["-"]
    selected_measurement = st.selectbox("กรุณาเลือก Measurement :", measurements, index=0)
    serial_numbers = []
    unique_serial_numbers = ["-"]
    selected_sn = st.selectbox("กรุณาเลือก Serial No. :", unique_serial_numbers, disabled=True)
else:
    measurements = ["-"] + measurements
    selected_measurement = st.selectbox("กรุณาเลือก Measurement :", measurements, index=0)
    serial_numbers = []
    if client and selected_measurement != "-":
        serial_numbers = get_serial_numbers(client, selected_measurement)
    def serial_sort_key(sn):
        try:
            parts = sn.split('-')
            if len(parts) >= 3:
                month = int(parts[1][:2])
                year = int(parts[1][2:])
                return (year, month, sn)
        except Exception:
            pass
        return (0, 0, sn)

    unique_serial_numbers = sorted(set(serial_numbers), key=serial_sort_key) if serial_numbers else ["-"]
    selected_sn = st.selectbox("กรุณาเลือก Serial No. :", unique_serial_numbers)

st.write(f"Measurement ที่เลือก : {selected_measurement}")

st.write(f"Serial No. ที่เลือก : {selected_sn}")

if 'csv_files' not in st.session_state:
    st.session_state.csv_files = {}

def build_query(measurement, serial_no, start_unix, end_unix):
    # InfluxDB ใช้ ms
    query = f'''
    SELECT mean("a1") AS "s1", mean("a2") AS "s2", mean("a3") AS "s3", mean("a4") AS "s4",
           mean("a5") AS "s5", mean("a6") AS "s6", mean("a7") AS "s7", mean("a8") AS "s8"
    FROM "{measurement}"
    WHERE ("sn" =~ /^({serial_no})$/)
      AND time >= {start_unix}000ms AND time <= {end_unix}000ms
    GROUP BY time(1m) fill(none)
    '''
    return query

def query_to_dataframe(client, query):
    try:
        result = client.query(query)
        # get_points อาจ error ถ้าไม่มี series
        points = []
        for serie in result.raw.get('series', []):
            for v in serie.get('values', []):
                # Map columns to values
                row = dict(zip(serie['columns'], v))
                points.append(row)
        if not points:
            return pd.DataFrame(columns=["Time", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "Smell"])
        df = pd.DataFrame(points)
        # Rename time column
        df.rename(columns={"time": "Time"}, inplace=True)
        # Convert time - InfluxDB returns UTC time, convert to Bangkok timezone properly
        df["Time"] = pd.to_datetime(df["Time"], utc=True).dt.tz_convert('Asia/Bangkok').dt.strftime('%d/%m/%Y  %H:%M:%S')
        # Only keep s1-s8
        for col in ["s1","s2","s3","s4","s5","s6","s7","s8"]:
            if col in df.columns:
                df[col] = df[col].round().astype('Int64').astype(str).replace('<NA>', '')
            else:
                df[col] = ''
        # Add Smell column
        df["Smell"] = ""
        # Reorder columns
        df = df[["Time", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "Smell"]]
        return df
    except Exception as e:
        st.error(f"[ERROR] Query failed: {e}")
        return pd.DataFrame(columns=["Time", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "Smell"])

if serial_numbers:
    sn_counter = Counter(serial_numbers)
    duplicate_count = sum(1 for v in sn_counter.values() if v > 1)
    unique_count = len(sn_counter)
    st.write(f"Serial No. ที่แสดงใน dropdown : {len(unique_serial_numbers)} ตัว | จากการ Query ทั้งหมด : {len(serial_numbers)} ตัว | ไม่ซ้ำ : {unique_count} ตัว | ซ้ำ : {duplicate_count} ตัว")
else:
    st.write("Serial No. ที่แสดงใน dropdown : 0 ตัว | จากการ Query ทั้งหมด : 0 ตัว | ไม่ซ้ำ : 0 ตัว | ซ้ำ : 0 ตัว")

st.markdown("")

# ปุ่ม Export to CSV
if st.button("Export to CSV", type="primary"):
    if not selected_measurement or not selected_sn:
        st.warning("กรุณาเลือก Measurement และ Serial No. ก่อน export")
    else:
        query = build_query(selected_measurement, selected_sn, start_unix, end_unix)
        df = query_to_dataframe(client, query)
        if df.empty:
            st.warning("ไม่พบข้อมูลสำหรับช่วงเวลานี้")
        else:
            st.session_state['edit_df'] = df.copy()
            st.session_state['edit_filename'] = f"export_{selected_measurement}_{selected_sn}_{start_unix}_{end_unix}.csv"

# แก้ไข Smell และบันทึกไฟล์ลง memory (st.data_editor)
if 'edit_df' in st.session_state:
    st.markdown("---")
    st.subheader("แก้ไข Smell Model")
    edited_df = st.data_editor(st.session_state['edit_df'], num_rows="dynamic", use_container_width=True, key="edit_table")
    if st.button("กำหนดชื่อกลิ่น", type="primary"):
        edited_df["Smell"] = edited_df["Smell"].fillna("")
        csv_buffer = io.StringIO()
        edited_df.to_csv(csv_buffer, index=False, encoding="utf-8-sig")
        fixed_filename = "smell_label.csv"
        st.session_state.csv_files[fixed_filename] = csv_buffer.getvalue()
        st.success(f"✅ บันทึกไฟล์ {fixed_filename} ในระบบแล้ว")
        # set flag เพื่อแสดง smell name editor
        st.session_state['show_smell_name_editor'] = True

# --- ส่วนนี้แยกออกมา ---
if st.session_state.get('show_smell_name_editor', False):
    df = pd.read_csv(io.StringIO(st.session_state.csv_files["smell_label.csv"]))
    unique_smells = pd.DataFrame({"Smell": sorted(df["Smell"].dropna().unique()), "Name": ""})
    st.markdown("---")
    st.subheader("กำหนดชื่อกลิ่น (Smell Name Mapping)")
    name_df = st.data_editor(unique_smells, num_rows="dynamic", use_container_width=True, key="smell_name_editor")
    if st.button("💾 บันทึกชื่อกลิ่น", key="save_smell_name"):
        excel_buffer = io.BytesIO()
        name_df.to_excel(excel_buffer, index=False)
        st.session_state.csv_files["smell_Name.xlsx"] = excel_buffer.getvalue()
        st.success("✅ บันทึกไฟล์ smell_Name.xlsx ในระบบแล้ว")

# แสดงไฟล์ใน Memory
if st.session_state.csv_files:
    st.markdown("---")
    st.subheader("💾 ไฟล์ใน Memory")
    with st.expander(f"📁 ไฟล์ทั้งหมด ({len(st.session_state.csv_files)} ไฟล์)", expanded=False):
        for filename, content in st.session_state.csv_files.items():
            st.markdown(f"📄 **{filename}** ({len(content)} ตัวอักษร)")
    if st.button("🗑️ ล้างไฟล์ทั้งหมดใน Memory", type="secondary"):
        st.session_state.csv_files = {}
        st.session_state.pop('show_smell_name_editor', None)
        st.session_state.pop('edit_df', None)
        st.session_state.pop('edit_filename', None)
        st.success("✅ ล้างไฟล์ใน Memory แล้ว")
        st.rerun()

# --- Plot Model ---
if "smell_label.csv" in st.session_state.csv_files and "smell_Name.xlsx" in st.session_state.csv_files:
    st.markdown("---")
    st.subheader("🔬 Plot Model (สร้างผลลัพธ์ทั้งหมด)")
    if st.button("Plot Model", type="primary"):
        outputs = processDataset.process_smell_label(
            st.session_state.csv_files["smell_label.csv"],
            io.BytesIO(st.session_state.csv_files["smell_Name.xlsx"])
        )

        # แสดงตาราง CSV
        st.markdown("#### sorted_labeled_data.csv")
        st.dataframe(pd.read_csv(io.StringIO(outputs["sorted_labeled_data.csv"])))
        st.markdown("#### dataset.csv")
        st.dataframe(pd.read_csv(io.StringIO(outputs["dataset.csv"])))
        st.markdown("#### average_smell_sensor_values.csv")
        st.dataframe(pd.read_csv(io.StringIO(outputs["average_smell_sensor_values.csv"])))

        # แสดง radar chart
        st.markdown("#### Radar Chart (PNG)")
        radar_files = [k for k in outputs if k.startswith("radarPlot/") and k.endswith(".png")]
        for fname in sorted(radar_files):
            st.image(outputs[fname], caption=fname, use_container_width=True)

        # ปุ่มดาวน์โหลด zip
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zf:
            for fname, content in outputs.items():
                zf.writestr(fname, content if isinstance(content, bytes) else content.encode("utf-8"))
        st.download_button("Download All Output (ZIP)", data=zip_buffer.getvalue(), file_name="smell_model_outputs.zip")

# กัน SQL Injection
if selected_measurement not in measurements:
    st.error("Measurement ไม่ถูกต้อง")
    st.stop()

if selected_sn not in unique_serial_numbers:
    st.error("Serial No. ไม่ถูกต้อง")
    st.stop()






