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
        # ใช้ SHOW TAG VALUES แบบเดียวกับ Grafana เพื่อดึง serial number ทั้งหมด
        query = f'SHOW TAG VALUES FROM "{measurement}" WITH KEY = "sn"'
        result = client.query(query)
        serials = [point['value'] for point in result.get_points()]
        return serials
    except Exception as e:
        print(f"[ERROR] Failed to query serial numbers: {e}")
        return []

# ฟังก์ชันดึง Station Names (sName) จาก measurement
def get_station_names(client, measurement):
    try:
        query = f'SHOW TAG VALUES FROM "{measurement}" WITH KEY = "sName"'
        result = client.query(query)
        stations = [point['value'] for point in result.get_points()]
        return stations
    except Exception as e:
        print(f"[ERROR] Failed to query station names: {e}")
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

# ฟังก์ชัน sort serial number
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

if not measurements:
    measurements = ["-"]
    selected_measurement = st.selectbox("กรุณาเลือก Measurement :", measurements, index=0)
    serial_numbers = []
    unique_serial_numbers = ["-"]
    selected_sn = st.selectbox("กรุณาเลือก Serial No. :", unique_serial_numbers, disabled=True)
    selected_station = None
else:
    measurements = ["-"] + measurements
    selected_measurement = st.selectbox("กรุณาเลือก Measurement :", measurements, index=0)
    serial_numbers = []
    if client and selected_measurement != "-":
        serial_numbers = get_serial_numbers(client, selected_measurement)
    
    # เพิ่มตัวเลือก "ไม่เจอ" ลงใน dropdown
    unique_serial_numbers = sorted(set(serial_numbers), key=serial_sort_key) if serial_numbers else []
    if unique_serial_numbers:
        unique_serial_numbers = ["-"] + unique_serial_numbers + ["❌ ไม่เจอ - ค้นหาจาก Station"]
    else:
        unique_serial_numbers = ["-", "❌ ไม่เจอ - ค้นหาจาก Station"]
    
    selected_sn = st.selectbox("กรุณาเลือก Serial No. :", unique_serial_numbers)
    
    # ถ้าเลือก "ไม่เจอ" ให้แสดง dropdown Station
    selected_station = None
    if selected_sn == "❌ ไม่เจอ - ค้นหาจาก Station":
        if client and selected_measurement != "-":
            station_names = get_station_names(client, selected_measurement)
            unique_stations = sorted(set(station_names)) if station_names else ["-"]
            if unique_stations and unique_stations != ["-"]:
                unique_stations = ["-"] + unique_stations
            selected_station = st.selectbox("🔍 กรุณาเลือก Station (sName) :", unique_stations)
            if selected_station != "-":
                st.info(f"💡 ระบบจะใช้ Station: **{selected_station}** ในการ query ข้อมูล")
                # ใช้ selected_station เป็น serial number แทน
                selected_sn = selected_station
        else:
            st.warning("⚠️ กรุณาเลือก Measurement ก่อน")
            selected_sn = "-"

st.write(f"Measurement ที่เลือก : {selected_measurement}")
if selected_station:
    st.write(f"🔍 ค้นหาจาก Station : {selected_station}")
else:
    st.write(f"Serial No. ที่เลือก : {selected_sn}")

if 'csv_files' not in st.session_state:
    st.session_state.csv_files = {}

# Initialize session state for splits
if 'splits' not in st.session_state:
    st.session_state.splits = []
if 'num_splits' not in st.session_state:
    st.session_state.num_splits = 1
if 'show_split_config' not in st.session_state:
    st.session_state.show_split_config = False

def build_query(measurement, serial_no, start_unix, end_unix, use_station=False):
    # InfluxDB ใช้ ms
    # ถ้า use_station=True จะใช้ sName แทน sn ในการ query
    tag_key = "sName" if use_station else "sn"
    query = f'''
    SELECT mean("a1") AS "s1", mean("a2") AS "s2", mean("a3") AS "s3", mean("a4") AS "s4",
           mean("a5") AS "s5", mean("a6") AS "s6", mean("a7") AS "s7", mean("a8") AS "s8"
    FROM "{measurement}"
    WHERE ("{tag_key}" =~ /^({serial_no})$/)
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
    # นับเฉพาะ serial number จริง (ไม่รวม "-" และ "❌ ไม่เจอ...")
    actual_sn_count = len([sn for sn in unique_serial_numbers if sn != "-" and not sn.startswith("❌")])
    st.write(f"Serial No. ที่แสดงใน dropdown : {actual_sn_count} ตัว | จากการ Query ทั้งหมด : {len(serial_numbers)} ตัว | ไม่ซ้ำ : {unique_count} ตัว | ซ้ำ : {duplicate_count} ตัว")
else:
    st.write("Serial No. ที่แสดงใน dropdown : 0 ตัว | จากการ Query ทั้งหมด : 0 ตัว | ไม่ซ้ำ : 0 ตัว | ซ้ำ : 0 ตัว")

st.markdown("")

# ปุ่ม Export to CSV
if st.button("Export to CSV", type="primary"):
    # Validation ที่ปรับปรุงแล้ว
    if not selected_measurement or selected_measurement == "-":
        st.warning("⚠️ กรุณาเลือก Measurement ก่อน export")
    elif not selected_sn or selected_sn == "-" or selected_sn == "❌ ไม่เจอ - ค้นหาจาก Station":
        st.warning("⚠️ กรุณาเลือก Serial No. หรือ Station ก่อน export")
    else:
        st.session_state.show_split_config = True
        st.session_state.selected_measurement = selected_measurement
        st.session_state.selected_sn = selected_sn
        st.session_state.use_station = (selected_station is not None)
        st.rerun()

# แสดงส่วน Split Configuration
if st.session_state.get('show_split_config', False):
    st.markdown("---")
    st.subheader("⚙️ กำหนด Time Range Splits")
    
    # เลือกจำนวน splits
    num_splits = st.number_input("จำนวน Split ที่ต้องการ:", min_value=1, max_value=20, value=st.session_state.num_splits, step=1)
    st.session_state.num_splits = num_splits
    
    # Initialize splits list if needed
    if len(st.session_state.splits) != num_splits:
        st.session_state.splits = [
            {
                'start_date': start_date,
                'start_time': start_time,
                'end_date': end_date,
                'end_time': end_time,
                'smell_label': f'Smell {i+1}',
                'smell_name': ''
            } for i in range(num_splits)
        ]
    
    # แสดงฟอร์มสำหรับแต่ละ split
    for i in range(num_splits):
        with st.expander(f"📋 Split {i+1}", expanded=(i==0)):
            col1, col2 = st.columns(2)
            with col1:
                split_start_date = st.date_input(
                    f"วันที่เริ่มต้น (Split {i+1})",
                    value=st.session_state.splits[i]['start_date'],
                    min_value=start_date,
                    max_value=end_date,
                    key=f"split_{i}_start_date"
                )
                split_start_time = st.time_input(
                    f"เวลาเริ่มต้น (Split {i+1})",
                    value=st.session_state.splits[i]['start_time'],
                    key=f"split_{i}_start_time"
                )
            with col2:
                split_end_date = st.date_input(
                    f"วันที่สิ้นสุด (Split {i+1})",
                    value=st.session_state.splits[i]['end_date'],
                    min_value=start_date,
                    max_value=end_date,
                    key=f"split_{i}_end_date"
                )
                split_end_time = st.time_input(
                    f"เวลาสิ้นสุด (Split {i+1})",
                    value=st.session_state.splits[i]['end_time'],
                    key=f"split_{i}_end_time"
                )
            
            col3, col4 = st.columns(2)
            with col3:
                smell_label = st.text_input(
                    f"Smell Label (Split {i+1})",
                    value=st.session_state.splits[i]['smell_label'],
                    key=f"split_{i}_smell_label",
                )
            with col4:
                smell_name = st.text_input(
                    f"ชื่อกลิ่น (Split {i+1})",
                    value=st.session_state.splits[i]['smell_name'],
                    key=f"split_{i}_smell_name",
                )
            
            # Update session state
            st.session_state.splits[i] = {
                'start_date': split_start_date,
                'start_time': split_start_time,
                'end_date': split_end_date,
                'end_time': split_end_time,
                'smell_label': smell_label,
                'smell_name': smell_name
            }
    
    st.markdown("")
    
    # ปุ่ม Process All Splits
    if st.button("✅ Process All Splits", type="primary"):
        # Validate all splits
        validation_errors = []
        for i, split in enumerate(st.session_state.splits):
            # Check if smell_name is filled
            if not split['smell_name'] or split['smell_name'].strip() == '':
                validation_errors.append(f"❌ Split {i+1}: กรุณากรอกชื่อกลิ่น")
            
            # Check if time range is within main time range
            split_start_dt = datetime.combine(split['start_date'], split['start_time'])
            split_end_dt = datetime.combine(split['end_date'], split['end_time'])
            main_start_dt = datetime.combine(start_date, start_time)
            main_end_dt = datetime.combine(end_date, end_time)
            
            if split_start_dt < main_start_dt:
                validation_errors.append(f"❌ Split {i+1}: เวลาเริ่มต้นต้องไม่น้อยกว่า {start_time.strftime('%H:%M')}")
            if split_end_dt > main_end_dt:
                validation_errors.append(f"❌ Split {i+1}: เวลาสิ้นสุดต้องไม่เกิน {end_time.strftime('%H:%M')}")
            if split_start_dt >= split_end_dt:
                validation_errors.append(f"❌ Split {i+1}: เวลาเริ่มต้นต้องน้อยกว่าเวลาสิ้นสุด")
        
        # Show validation errors
        if validation_errors:
            for error in validation_errors:
                st.error(error)
        else:
            # Process if validation passes
            all_dfs = []
            smell_name_mapping = {}
            
            # Process each split
            for i, split in enumerate(st.session_state.splits):
                # Convert to datetime
                split_start_dt = bangkok_tz.localize(datetime.combine(split['start_date'], split['start_time']))
                split_end_dt = bangkok_tz.localize(datetime.combine(split['end_date'], split['end_time']))
                split_start_unix = int(split_start_dt.timestamp())
                split_end_unix = int(split_end_dt.timestamp())
                
                # Query data
                use_station = st.session_state.get('use_station', False)
                query = build_query(st.session_state.selected_measurement, st.session_state.selected_sn, split_start_unix, split_end_unix, use_station)
                df = query_to_dataframe(client, query)
                
                if not df.empty:
                    # Set Smell label for entire split
                    df['Smell'] = split['smell_label']
                    all_dfs.append(df)
                    
                    # Store smell name mapping
                    smell_name_mapping[split['smell_label']] = split['smell_name']
            
            # Concatenate all dataframes (รันครั้งเดียวหลังจาก loop เสร็จ)
            if all_dfs:
                combined_df = pd.concat(all_dfs, ignore_index=True)
                
                # Save to session state
                csv_buffer = io.StringIO()
                combined_df.to_csv(csv_buffer, index=False, encoding="utf-8-sig")
                st.session_state.csv_files["smell_label.csv"] = csv_buffer.getvalue()
                
                # Create smell name mapping Excel
                name_df = pd.DataFrame([
                    {'Smell': k, 'Name': v} for k, v in smell_name_mapping.items()
                ])
                excel_buffer = io.BytesIO()
                name_df.to_excel(excel_buffer, index=False)
                st.session_state.csv_files["smell_Name.xlsx"] = excel_buffer.getvalue()
                
                st.success(f"✅ ประมวลผลสำเร็จ! รวมข้อมูล {len(all_dfs)} splits ({len(combined_df)} แถว)")
                
                # Show final combined table only
                st.markdown("#### 👀 ตัวอย่างข้อมูลที่รวมแล้ว (Final)")
                st.dataframe(combined_df, use_container_width=True)
                
                st.markdown("#### 📝 Smell Name Mapping")
                st.dataframe(name_df, use_container_width=True)
            else:
                st.error("❌ ไม่พบข้อมูลในช่วงเวลาที่เลือก")

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
        st.session_state.pop('show_split_config', None)
        st.session_state.pop('use_station', None)
        st.session_state.pop('selected_measurement', None)
        st.session_state.pop('selected_sn', None)
        st.session_state.splits = []
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

        # แสดง PCA plot
        st.markdown("#### PCA Analysis (2D Scatter Plot)")
        pca_files = [k for k in outputs if k.startswith("pcaPlot/") and k.endswith(".png")]
        for fname in sorted(pca_files):
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

# ตรวจสอบ Serial No. หรือ Station
if selected_station:
    # ถ้าใช้ Station ให้ตรวจว่า station ถูกต้องหรือไม่
    if client and selected_measurement != "-":
        valid_stations = get_station_names(client, selected_measurement)
        if selected_station not in valid_stations:
            st.error("Station ไม่ถูกต้อง")
            st.stop()
else:
    # ถ้าใช้ Serial No. ตรวจตามเดิม
    if selected_sn not in unique_serial_numbers:
        st.error("Serial No. ไม่ถูกต้อง")
        st.stop()






