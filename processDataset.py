import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import io
import re
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from scipy.cluster.hierarchy import dendrogram, linkage
from scipy.spatial.distance import pdist

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

    # --- Step 3: PCA Analysis ---
    pca_outputs = {}
    
    # Extract features (s1-s8) for PCA
    X = average_with_names[sensor_labels].values
    smell_labels = average_with_names['Smell'].values
    name_labels = average_with_names['Name'].fillna(average_with_names['Smell']).values
    
    # Standardize the features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Apply PCA (2 components for 2D visualization)
    pca = PCA(n_components=min(2, len(X)))
    principal_components = pca.fit_transform(X_scaled)
    
    # Create PCA results dataframe
    pca_df = pd.DataFrame({
        'Smell': smell_labels,
        'Name': name_labels,
        'PC1': principal_components[:, 0].round(3),
        'PC2': principal_components[:, 1].round(3) if principal_components.shape[1] > 1 else 0
    })
    
    # Save PCA results
    buf_pca = io.StringIO()
    pca_df.to_csv(buf_pca, index=False)
    pca_outputs['pca_results.csv'] = buf_pca.getvalue()
    
    # Save explained variance
    variance_df = pd.DataFrame({
        'Component': [f'PC{i+1}' for i in range(len(pca.explained_variance_ratio_))],
        'Explained_Variance_Ratio': (pca.explained_variance_ratio_ * 100).round(2)
    })
    buf_var = io.StringIO()
    variance_df.to_csv(buf_var, index=False)
    pca_outputs['pca_variance.csv'] = buf_var.getvalue()
    
    # Save component loadings (weights)
    loadings_df = pd.DataFrame(
        pca.components_.T,
        columns=[f'PC{i+1}' for i in range(len(pca.components_))],
        index=sensor_labels
    ).round(3)
    loadings_df.insert(0, 'Sensor', loadings_df.index)
    buf_load = io.StringIO()
    loadings_df.to_csv(buf_load, index=False)
    pca_outputs['pca_components.csv'] = buf_load.getvalue()
    
    # Generate PCA 2D scatter plot
    fig, ax = plt.subplots(figsize=(10, 8))
    
    # Plot each smell with different colors
    colors = plt.cm.tab10(np.linspace(0, 1, len(smell_labels)))
    for i, (smell, name) in enumerate(zip(smell_labels, name_labels)):
        ax.scatter(
            principal_components[i, 0],
            principal_components[i, 1] if principal_components.shape[1] > 1 else 0,
            c=[colors[i]],
            s=200,
            alpha=0.7,
            edgecolors='black',
            linewidth=2,
            label=name
        )
        ax.annotate(
            name,
            (principal_components[i, 0], principal_components[i, 1] if principal_components.shape[1] > 1 else 0),
            fontsize=10,
            fontweight='bold',
            ha='center',
            va='bottom',
            xytext=(0, 10),
            textcoords='offset points'
        )
    
    ax.set_xlabel(f'PC1 ({pca.explained_variance_ratio_[0]*100:.1f}%)', fontsize=12, fontweight='bold')
    if len(pca.explained_variance_ratio_) > 1:
        ax.set_ylabel(f'PC2 ({pca.explained_variance_ratio_[1]*100:.1f}%)', fontsize=12, fontweight='bold')
    else:
        ax.set_ylabel('PC2 (0%)', fontsize=12, fontweight='bold')
    ax.set_title('PCA Analysis', fontsize=14, fontweight='bold', pad=20)
    ax.grid(True, alpha=0.3)
    ax.axhline(y=0, color='k', linestyle='--', linewidth=0.5)
    ax.axvline(x=0, color='k', linestyle='--', linewidth=0.5)
    ax.legend(loc='upper right', fontsize=9, framealpha=0.9, 
              bbox_to_anchor=(1.0, 1.0), borderaxespad=0.5,
              handletextpad=1.0, labelspacing=1.2)
    
    img_buf_pca = io.BytesIO()
    plt.tight_layout(pad=1.5)
    plt.savefig(img_buf_pca, format='png', dpi=150)
    plt.close(fig)
    pca_outputs['pcaPlot/pca_scatter_2d.png'] = img_buf_pca.getvalue()

    # --- HCA (Hierarchical Cluster Analysis) ---
    hca_outputs = {}
    
    # Use the same standardized data as PCA
    X_scaled = scaler.fit_transform(average_with_names[sensor_labels])
    
    # Compute linkage matrix using Ward's method
    linkage_matrix = linkage(X_scaled, method='ward')
    
    # Create dendrogram
    fig_hca, ax_hca = plt.subplots(figsize=(12, 6))
    dendrogram(
        linkage_matrix,
        labels=name_labels,
        ax=ax_hca,
        leaf_font_size=11,
        leaf_rotation=45,
        orientation='top',
        color_threshold=0.7 * max(linkage_matrix[:, 2])
    )
    ax_hca.set_ylabel('Distance', fontsize=12, fontweight='bold')
    ax_hca.set_xlabel('Sample Index or Label', fontsize=12, fontweight='bold')
    ax_hca.set_title('Hierarchical Clustering Dendrogram', fontsize=14, fontweight='bold', pad=20)
    ax_hca.grid(True, alpha=0.3, axis='y')
    
    img_buf_hca = io.BytesIO()
    plt.tight_layout()
    plt.savefig(img_buf_hca, format='png', dpi=150)
    plt.close(fig_hca)
    hca_outputs['hcaPlot/hca_dendrogram.png'] = img_buf_hca.getvalue()
    
    # Save linkage matrix
    linkage_df = pd.DataFrame(
        linkage_matrix,
        columns=['Cluster1', 'Cluster2', 'Distance', 'Sample_Count']
    )
    buf_linkage = io.StringIO()
    linkage_df.to_csv(buf_linkage, index=False)
    hca_outputs['hca_linkage_matrix.csv'] = buf_linkage.getvalue()

    # Return all files as dict
    return {
        "sorted_labeled_data.csv": buf_sorted.getvalue(),
        "dataset.csv": buf_dataset.getvalue(),
        "average_smell_sensor_values.csv": buf_avg.getvalue(),
        **radar_imgs,
        **pca_outputs,
        **hca_outputs
    }

