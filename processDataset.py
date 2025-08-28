import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import io
import re

def process_smell_label(smell_label_csv, smell_name_excel):
    """
    smell_label_csv: str (csv) หรือ BytesIO
    smell_name_excel: BytesIO (excel)
    return: dict {filename: content}
    """
    # --- Step 1: Extract and Sort Labeled Data ---
    df = pd.read_csv(io.StringIO(smell_label_csv) if isinstance(smell_label_csv, str) else smell_label_csv)
    filtered_df = df[df['Smell'].notna() & (df['Smell'].astype(str).str.strip() != '')]
    labels = filtered_df['Smell'].dropna().unique()

    def smell_sort_key(label):
        if label == 'Air Zero':
            return 0
        match = re.match(r"Smell(\d+)", label)
        return int(match.group(1)) if match else float('inf')

    sorted_labels = sorted(labels, key=smell_sort_key)
    filtered_df['Smell'] = pd.Categorical(filtered_df['Smell'], categories=sorted_labels, ordered=True)
    sorted_df = filtered_df.sort_values('Smell')

    # Save sorted_labeled_data.csv (in memory)
    buf_sorted = io.StringIO()
    sorted_df.to_csv(buf_sorted, index=False)

    # Save only s1 to s8 and Smell columns to a new file (in memory)
    columns_to_keep = ['s1', 's2', 's3', 's4', 's5','s6', 's7', 's8', 'Smell']
    dataset_df = sorted_df[columns_to_keep]
    buf_dataset = io.StringIO()
    dataset_df.to_csv(buf_dataset, index=False)

    # --- Step 2: Generate Radar Charts from Sorted Data ---
    avg_values = dataset_df.groupby('Smell').mean(numeric_only=True)
    avg_values_rounded = avg_values.round(2)
    avg_values_rounded = avg_values_rounded.reset_index()

    # Load smell names from Excel
    name_map_df = pd.read_excel(smell_name_excel)
    name_map_df = name_map_df[['Smell', 'Name']]

    # Merge names into the average values table
    average_with_names = pd.merge(avg_values_rounded, name_map_df, on='Smell', how='left')
    cols = ['Smell', 'Name'] + [col for col in average_with_names.columns if col not in ['Smell', 'Name']]
    average_with_names = average_with_names[cols]

    # Save the new table (in memory)
    buf_avg = io.StringIO()
    average_with_names.to_csv(buf_avg, index=False)

    # Prepare radar chart (in memory)
    sensor_labels = [col for col in average_with_names.columns if col not in ['Smell', 'Name']]
    num_vars = len(sensor_labels)
    angles = np.linspace(0, 2 * np.pi, num_vars, endpoint=False).tolist()
    angles += angles[:1]

    radar_imgs = {}
    for _, row in average_with_names.iterrows():
        values = row[sensor_labels].tolist()
        values += values[:1]
        name = row['Name'] if pd.notna(row['Name']) else row['Smell']
        smell_code = row['Smell']

        fig, ax = plt.subplots(figsize=(6, 6), subplot_kw=dict(polar=True))
        ax.set_theta_offset(np.pi / 2) #type: ignore
        ax.set_theta_direction(-1) #type: ignore
        ax.set_ylim(0, 1024)
        ax.plot(angles, values, marker='o')
        ax.fill(angles, values, alpha=0.25)
        ax.set_xticks(angles[:-1])
        ax.set_xticklabels(sensor_labels)
        ax.set_title(name, fontproperties="Tahoma", fontsize=14)
        safe_filename = re.sub(r'[^\w\-_.]', '_', str(smell_code))
        img_buf = io.BytesIO()
        plt.tight_layout()
        plt.savefig(img_buf, format='png')
        plt.close(fig)
        radar_imgs[f"radarPlot/radar_chart_{safe_filename}.png"] = img_buf.getvalue()

    # Return all files as dict
    return {
        "sorted_labeled_data.csv": buf_sorted.getvalue(),
        "dataset.csv": buf_dataset.getvalue(),
        "average_smell_sensor_values.csv": buf_avg.getvalue(),
        **radar_imgs
    }

