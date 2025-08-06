"""
Contact List Consolidation Pipeline - Streamlit App

A user-friendly web interface for the contact data processing pipeline.
Upload files, run the pipeline, and download results with one click.
"""

import streamlit as st
import subprocess
import time
import os
from pathlib import Path
import shutil
import zipfile
from datetime import datetime
import io
import psutil

# Page configuration
st.set_page_config(
    page_title="Contact List Consolidator",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'pipeline_running' not in st.session_state:
    st.session_state.pipeline_running = False
if 'pipeline_completed' not in st.session_state:
    st.session_state.pipeline_completed = False
if 'pipeline_success' not in st.session_state:
    st.session_state.pipeline_success = False
if 'pipeline_logs' not in st.session_state:
    st.session_state.pipeline_logs = []
if 'pipeline_process' not in st.session_state:
    st.session_state.pipeline_process = None

def clear_datasource_folder():
    """Clear the DataSource folder before new uploads"""
    datasource_path = Path("DataSource")
    if datasource_path.exists():
        for item in datasource_path.iterdir():
            if item.is_file():
                item.unlink()
            elif item.is_dir():
                shutil.rmtree(item)

def save_uploaded_files(uploaded_files):
    """Save uploaded files to DataSource folder"""
    datasource_path = Path("DataSource")
    datasource_path.mkdir(exist_ok=True)
    
    saved_files = []
    for uploaded_file in uploaded_files:
        file_path = datasource_path / uploaded_file.name
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        saved_files.append(uploaded_file.name)
    
    return saved_files

def get_datasource_files():
    """Get list of files currently in DataSource folder"""
    datasource_path = Path("DataSource")
    if not datasource_path.exists():
        return []
    
    files = []
    for file_path in datasource_path.iterdir():
        if file_path.is_file():
            files.append({
                'name': file_path.name,
                'size': file_path.stat().st_size,
                'path': file_path
            })
    
    return sorted(files, key=lambda x: x['name'])

def delete_datasource_file(file_path):
    """Delete a specific file from DataSource folder"""
    try:
        file_path.unlink()
        return True
    except:
        return False

def get_current_agent_status():
    """Extract current agent from recent logs"""
    if not st.session_state.pipeline_logs:
        return "Ready"
    
    # Look for agent indicators in recent logs
    recent_logs = st.session_state.pipeline_logs[-10:]  # Check last 10 log lines
    
    for log_line in reversed(recent_logs):
        log_lower = log_line.lower()
        if 'agent 1' in log_lower or 'file_converter' in log_lower:
            return "🔄 Agent 1: File Converter"
        elif 'agent 2' in log_lower or 'column_mapper' in log_lower:
            return "🔄 Agent 2: Column Mapper"
        elif 'agent 3' in log_lower or 'data_consolidator' in log_lower:
            return "🔄 Agent 3: Data Consolidator"
        elif 'agent 4' in log_lower or 'smart_deduplicator' in log_lower:
            return "🔄 Agent 4: Smart Deduplicator"
        elif 'agent 5' in log_lower or 'web_scraper' in log_lower:
            return "🔄 Agent 5: Email Enrichment"
        elif 'agent 6' in log_lower or 'csv_cleaner' in log_lower:
            return "🔄 Agent 6: CRM Exporter"
    
    if st.session_state.pipeline_running:
        return "🔄 Pipeline Running"
    elif st.session_state.pipeline_completed:
        return "✅ Pipeline Complete" if st.session_state.pipeline_success else "❌ Pipeline Failed"
    else:
        return "📋 Ready"

def start_pipeline():
    """Start the pipeline process"""
    try:
        # Clear previous state
        st.session_state.pipeline_logs = []
        st.session_state.pipeline_running = True
        st.session_state.pipeline_completed = False
        st.session_state.pipeline_success = False
        st.session_state.log_file_position = 0
        
        # Start the pipeline process with output redirection to log file
        log_file = Path("pipeline_running.log")
        if log_file.exists():
            log_file.unlink()
        
        process = subprocess.Popen(
            ["python", "run_pipeline.py"],
            stdout=open(log_file, "w"),
            stderr=subprocess.STDOUT,
            universal_newlines=True
        )
        
        st.session_state.pipeline_process = process
        return True
        
    except Exception as e:
        st.session_state.pipeline_logs = [f"❌ ERROR starting pipeline: {str(e)}"]
        st.session_state.pipeline_running = False
        st.session_state.pipeline_completed = True
        st.session_state.pipeline_success = False
        return False

def check_pipeline_status():
    """Check pipeline status and read complete logs from start to end"""
    if not st.session_state.pipeline_running or not st.session_state.pipeline_process:
        return
    
    process = st.session_state.pipeline_process
    log_file = Path("pipeline_running.log")
    
    # Always read the complete log file to ensure we capture everything
    if log_file.exists():
        try:
            with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                complete_content = f.read()
                if complete_content:
                    # Store complete log, replacing previous content to avoid duplication
                    complete_lines = complete_content.strip().split('\n')
                    # Filter out empty lines but keep the structure
                    st.session_state.pipeline_logs = [line for line in complete_lines if line.strip()]
        except Exception as e:
            st.session_state.pipeline_logs.append(f"⚠️ Log read error: {str(e)}")
    
    # Check if process is still running
    if process.poll() is not None:
        # Process completed - read final complete log one more time
        if log_file.exists():
            try:
                with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                    final_content = f.read()
                    if final_content:
                        final_lines = final_content.strip().split('\n')
                        st.session_state.pipeline_logs = [line for line in final_lines if line.strip()]
            except:
                pass
        
        # Update status
        st.session_state.pipeline_running = False
        st.session_state.pipeline_completed = True
        st.session_state.pipeline_success = (process.returncode == 0)
        st.session_state.pipeline_process = None
        
        # Archive the complete log for persistence
        if log_file.exists():
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                archive_path = Path(f"logs/pipeline_log_archive_{timestamp}.log")
                log_file.rename(archive_path)
                st.session_state.pipeline_logs.append(f"📁 Complete log archived as: {archive_path.name}")
            except:
                # Clean up if archiving fails
                try:
                    log_file.unlink()
                except:
                    pass

def stop_pipeline():
    """Stop the currently running pipeline process"""
    if not st.session_state.pipeline_running or not st.session_state.pipeline_process:
        return False
    
    try:
        process = st.session_state.pipeline_process
        # Terminate the process (similar to Ctrl+Z)
        process.terminate()
        
        # Wait a moment for graceful termination
        try:
            process.wait(timeout=3)
        except:
            # If it doesn't terminate gracefully, force kill it
            process.kill()
            process.wait()
        
        # Update session state
        st.session_state.pipeline_running = False
        st.session_state.pipeline_completed = True
        st.session_state.pipeline_success = False
        st.session_state.pipeline_process = None
        
        # Add stop message to logs
        st.session_state.pipeline_logs.append("🛑 Pipeline stopped by user")
        st.session_state.pipeline_logs.append(f"⏹️ Process terminated at {datetime.now().strftime('%H:%M:%S')}")
        
        # Clean up log file
        log_file = Path("pipeline_running.log")
        if log_file.exists():
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                archive_path = Path(f"logs/pipeline_stopped_{timestamp}.log")
                log_file.rename(archive_path)
                st.session_state.pipeline_logs.append(f"📁 Partial log saved as: {archive_path.name}")
            except:
                try:
                    log_file.unlink()
                except:
                    pass
        
        return True
    except Exception as e:
        st.session_state.pipeline_logs.append(f"❌ Error stopping pipeline: {str(e)}")
        return False

def get_output_files():
    """Get list of output files from the output directory, focusing on Agent 6"""
    output_path = Path("output/agent_6_crm_exporter")
    if not output_path.exists():
        return []
    
    output_files = []
    
    # Priority order: Agent 6 first, then others
    priority_folders = [
        "agent_6_csv_cleaner_crm_exporter",  # Agent 6 - highest priority
    ]
    
    # First, get files from priority folders in order
    for folder_name in priority_folders:
        folder_path = output_path / folder_name
        if folder_path.exists():
            for item in folder_path.rglob("*"):
                if item.is_file() and not item.name.startswith('.'):
                    relative_path = item.relative_to(output_path)
                    output_files.append({
                        'path': item,
                        'relative_path': str(relative_path),
                        'size': item.stat().st_size,
                        'modified': datetime.fromtimestamp(item.stat().st_mtime),
                        'priority': priority_folders.index(folder_name) if folder_name in str(relative_path) else 999
                    })
    
    # Then get any other files not in priority folders
    for item in output_path.rglob("*"):
        if item.is_file() and not item.name.startswith('.'):
            relative_path = item.relative_to(output_path)
            # Skip if already added
            if not any(f['relative_path'] == str(relative_path) for f in output_files):
                output_files.append({
                    'path': item,
                    'relative_path': str(relative_path),
                    'size': item.stat().st_size,
                    'modified': datetime.fromtimestamp(item.stat().st_mtime),
                    'priority': 999
                })
    
    # Sort by priority first, then by modification time
    return sorted(output_files, key=lambda x: (x['priority'], -x['modified'].timestamp()))

def create_download_zip():
    """Create a ZIP file of all outputs for download"""
    output_path = Path("output")
    if not output_path.exists():
        return None
    
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for file_path in output_path.rglob("*"):
            if file_path.is_file() and not file_path.name.startswith('.'):
                arc_name = file_path.relative_to(output_path)
                zip_file.write(file_path, arc_name)
    
    zip_buffer.seek(0)
    return zip_buffer.getvalue()

# Main App Layout
def main():
    # Header
    st.title("📊 Contact List Consolidation Pipeline")
    st.markdown("### Transform and deduplicate your contact data with AI-powered processing")
    
    # Sidebar for controls
    st.sidebar.header("🚀 Pipeline Control")
    
    # Check if DataSource has files
    datasource_files = get_datasource_files()
    
    if datasource_files:
        st.sidebar.write(f"📂 Ready: {len(datasource_files)} files in DataSource")
        
        if not st.session_state.pipeline_running:
            if st.sidebar.button("🚀 RUN PIPELINE", type="primary", help="Execute the complete 6-agent pipeline"):
                # Start pipeline process
                if start_pipeline():
                    st.sidebar.success("✅ Pipeline started!")
                else:
                    st.sidebar.error("❌ Failed to start pipeline")
                st.rerun()
        else:
            st.sidebar.warning("⏳ Pipeline is running...")
            
            # Stop button when pipeline is running
            col1, col2 = st.sidebar.columns([1, 1])
            with col1:
                if st.button("🛑 STOP", type="secondary", help="Stop the running pipeline"):
                    if stop_pipeline():
                        st.sidebar.success("✅ Pipeline stopped!")
                    else:
                        st.sidebar.error("❌ Failed to stop pipeline")
                    st.rerun()
            with col2:
                if st.button("🔄 Refresh", help="Refresh logs and status"):
                    st.rerun()
    else:
        st.sidebar.warning("⚠️ No files in DataSource. Please upload files first.")
    
    st.sidebar.markdown("---")
    
    # File Management Section (moved to #2)
    st.sidebar.header("📁 File Management")
    st.sidebar.subheader("2. Upload & Manage Files")
    
    # File Upload
    uploaded_files = st.sidebar.file_uploader(
        "Choose CSV/Excel files",
        type=['csv', 'xlsx', 'xls'],
        accept_multiple_files=True,
        help="Upload contact data files in CSV or Excel format",
        key="file_uploader"
    )
    
    if uploaded_files:
        if st.sidebar.button("📤 Add to DataSource", type="secondary"):
            saved_files = save_uploaded_files(uploaded_files)
            st.sidebar.success(f"✅ Added {len(saved_files)} files!")
            st.rerun()
    
    # Current DataSource Files Display
    st.sidebar.subheader("🗂 Current DataSource Files")
    
    if datasource_files:
        for file_info in datasource_files:
            col1, col2 = st.sidebar.columns([3, 1])
            with col1:
                st.write(f"📄 {file_info['name']}")
                st.caption(f"Size: {file_info['size']:,} bytes")
            with col2:
                if st.button("🗑️", key=f"delete_{file_info['name']}", help="Delete file"):
                    if delete_datasource_file(file_info['path']):
                        st.sidebar.success(f"Deleted {file_info['name']}")
                        st.rerun()
                    else:
                        st.sidebar.error("Failed to delete file")
        
        # Clear all files option
        if st.sidebar.button("🗑️ Clear All Files", type="secondary"):
            clear_datasource_folder()
            st.sidebar.success("All files cleared!")
            st.rerun()
    else:
        st.sidebar.info("📂 No files in DataSource folder")
    
    # Main Content Area
    col1, col2 = st.columns([2, 1])
    
    # Get output files (focus on Agent 6)
    output_files = get_output_files()
    
    with col1:
        # Pipeline Status and Logs
        st.header("🔍 Pipeline Status & Logs")
        
        # Agent Status Indicator
        current_agent = get_current_agent_status()
        st.markdown(f"**Current Status:** {current_agent}")
        
        if st.session_state.pipeline_running:
            st.info("⏳ Pipeline is currently running...")
            progress_bar = st.progress(0.0)
            # Estimate progress based on log content (rough estimation)
            progress = min(len(st.session_state.pipeline_logs) / 100, 1.0)
            progress_bar.progress(progress)
        
        elif st.session_state.pipeline_completed:
            if st.session_state.pipeline_success:
                st.success("✅ Pipeline completed successfully!")
            else:
                st.error("❌ Pipeline failed. Check logs for details.")
        
        else:
            st.info("📋 Ready to process your contact files")
        
        # Check pipeline status and update logs
        if st.session_state.pipeline_running:
            check_pipeline_status()
        
        # Live Logs Display
        if st.session_state.pipeline_logs or st.session_state.pipeline_running:
            st.subheader("📜 Live Logs")
            logs_container = st.container()
            
            with logs_container:
                # Show latest logs in a scrollable text area
                if st.session_state.pipeline_logs:
                    log_text = "\n".join(st.session_state.pipeline_logs[-50:])  # Show last 50 lines
                else:
                    log_text = "Starting pipeline..."
                
                st.text_area(
                    "Pipeline Output",
                    value=log_text,
                    height=400,
                    help="Live output from the pipeline execution",
                    key="log_display"
                )
                
                if st.session_state.pipeline_running:
                    # Auto-refresh during pipeline execution
                    time.sleep(1)
                    st.rerun()
    
    with col2:
        # Output Files Browser
        st.header("📂 Output Files")
        
        if output_files:
            st.write(f"**{len(output_files)} files generated:**")
            
            for file_info in output_files[:10]:  # Show first 10 files
                with st.expander(f"📄 {file_info['relative_path']}"):
                    st.write(f"**Size:** {file_info['size']:,} bytes")
                    st.write(f"**Modified:** {file_info['modified'].strftime('%Y-%m-%d %H:%M:%S')}")
                    
                    # Download individual file
                    with open(file_info['path'], 'rb') as f:
                        file_data = f.read()
                    
                    st.download_button(
                        "📥 Download",
                        data=file_data,
                        file_name=file_info['path'].name,
                        mime="application/octet-stream",
                        key=f"download_{file_info['relative_path']}"
                    )
            
            if len(output_files) > 10:
                st.write(f"... and {len(output_files) - 10} more files")
        
        else:
            st.info("No output files generated yet")
    
    # Footer
    st.markdown("---")

if __name__ == "__main__":
    main()
