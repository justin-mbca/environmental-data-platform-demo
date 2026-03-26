"""
Streamlit dashboard for environmental trends and data quality summary.
"""
import streamlit as st
import pandas as pd
import os
import json

def load_gold_data():
    gold_dir = 'data/gold'
    files = [f for f in os.listdir(gold_dir) if f.endswith('.csv')]
    if not files:
        st.warning('No curated datasets found in gold layer.')
        return None
    df = pd.read_csv(os.path.join(gold_dir, files[-1]))
    return df

def load_quality_log():
    # Read from pipeline.log, which contains provenance and data quality info
    log_path = 'logs/pipeline.log'
    if not os.path.exists(log_path):
        return []
    with open(log_path) as f:
        return f.readlines()

def main():
    st.title('Environmental Data Platform Dashboard')
    st.markdown('---')
    st.markdown("""
    **Data Sources**
    - **CSV:** Environmental measurements (multiple locations and parameters). Generated as synthetic sample data in the pipeline to simulate field sensor readings.
    - **JSON:** Chemical/toxicology data (CAS numbers, chemical names, toxicity). Generated as synthetic sample data to represent chemical reference tables.
    - **EPA AirData API:** Real air quality data (Ozone, PM2.5, etc.) from the US Environmental Protection Agency, or fallback to mock data if API is unavailable. Data is fetched for a specific site and date range.
    
    Each record in the analytics table below includes a `source` column indicating its origin (csv, json, or api).
    """)
    st.markdown('---')
    st.markdown("""
    **Overview**
    
    This dashboard provides a unified view of environmental and toxicology data processed through a simulated Azure-style medallion architecture (Bronze, Silver, Gold layers). It displays curated, analytics-ready data, key statistics, and data quality logs. Use this dashboard to explore environmental trends, data quality, and platform health.
    """)
    st.markdown('---')
    df = load_gold_data()
    if df is not None:
        st.subheader('Curated Environmental Data (with Lineage)')
        # Show source/lineage column in the table
        display_cols = list(df.columns)
        if 'source' in display_cols:
            # Move 'source' to the front for visibility
            display_cols = ['source'] + [c for c in display_cols if c != 'source']
        st.dataframe(df[display_cols].head(100))
        # Summary statistics
        st.markdown('**Summary Statistics**')
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric('Total Records', len(df))
        with col2:
            st.metric('Unique Locations', df["location"].nunique() if "location" in df.columns else "-")
        with col3:
            st.metric('Parameters', df["parameter"].nunique() if "parameter" in df.columns else "-")
        with col4:
            if "timestamp" in df.columns:
                ts = pd.to_datetime(df['timestamp'], errors='coerce')
                if ts.notnull().any():
                    min_ts = ts.min()
                    max_ts = ts.max()
                    st.metric('Date Range', f"{min_ts} to {max_ts}")
                else:
                    st.metric('Date Range', '-')
            else:
                st.metric('Date Range', '-')
        st.markdown('---')
        # Parameter distribution
        if 'parameter' in df.columns and 'value' in df.columns:
            st.subheader('Average Value by Parameter')
            st.line_chart(df.groupby('parameter')['value'].mean())
            st.bar_chart(df.groupby('parameter')['value'].mean())
        # Location distribution
        if 'location' in df.columns and 'value' in df.columns:
            st.subheader('Average Value by Location')
            st.bar_chart(df.groupby('location')['value'].mean())
        # Value histogram
        if 'value' in df.columns:
            st.subheader('Value Distribution (Histogram)')
            import matplotlib.pyplot as plt
            fig, ax = plt.subplots()
            ax.hist(df['value'].dropna(), bins=20, color='skyblue', edgecolor='black')
            ax.set_xlabel('Value')
            ax.set_ylabel('Frequency')
            ax.set_title('Histogram of Value')
            st.pyplot(fig)
        # Pie chart for parameters
        if 'parameter' in df.columns:
            st.subheader('Parameter Distribution')
            st.write(df['parameter'].value_counts())
            st.pyplot(df['parameter'].value_counts().plot.pie(autopct='%1.1f%%', figsize=(4,4)).get_figure())
    st.markdown('---')
    st.subheader('Data Quality Log')
    log_lines = load_quality_log()
    if log_lines:
        st.text(''.join(log_lines[-20:]))
        # Highlight provenance/lineage info if present
        provenance_lines = [line for line in log_lines if 'provenance' in line.lower() or 'lineage' in line.lower()]
        if provenance_lines:
            st.markdown('**Provenance / Lineage Information**')
            provenance_block = ''.join(provenance_lines[-50:])
            st.markdown(
                '<div style="max-height: 300px; overflow-y: auto; border: 1px solid #ddd; border-radius: 4px; background: #f9f9f9; padding: 8px; font-family: monospace; font-size: 0.9em;">'
                + provenance_block.replace('<', '&lt;').replace('>', '&gt;').replace('\n', '<br>') +
                '</div>',
                unsafe_allow_html=True
            )
    else:
        st.info('No data quality issues logged.')

if __name__ == '__main__':
    main()
