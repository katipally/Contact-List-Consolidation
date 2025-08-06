# 📊 Contact List Consolidation Pipeline - Streamlit App

## 🚀 Quick Start

The Streamlit app provides a user-friendly web interface for the contact data processing pipeline. Upload files, run the pipeline, and download results with one click!

### Launch the App
```bash
streamlit run streamlit_app.py
```

Then open your browser to: **http://localhost:8501**

## 📋 How to Use

### Step 1: Upload Files 📁
- **Drag & drop** CSV/Excel files into the upload area
- Click **"Save to DataSource"** to upload files
- Files are automatically saved to the `DataSource/` folder

### Step 2: Run Pipeline 🚀
- Click the **"RUN PIPELINE"** button in the sidebar
- Watch **live logs** stream in real-time (exactly like `python run_pipeline.py`)
- Progress indicator shows pipeline status

### Step 3: Download Results 📂
- Browse all generated files in the **Output Files** section
- Download individual files or **all results as ZIP**
- Files are organized by agent output folders

## 🎯 Features

### ✅ **Perfect Integration**
- Uses `python run_pipeline.py` subprocess - **no import conflicts**
- Identical behavior to manual pipeline execution
- All 6 agents run seamlessly

### ✅ **Real-Time Monitoring**
- **Live log streaming** - see exactly what's happening
- Progress indicators and status updates
- Auto-refresh during pipeline execution

### ✅ **File Management**
- **Drag & drop uploads** to `DataSource/` folder
- **Automatic folder syncing** with pipeline
- **Download all outputs** as organized ZIP file

### ✅ **Error-Free Execution**
- Subprocess isolation prevents conflicts
- Proper error handling and status reporting
- Thread-safe execution model

## 📁 Folder Structure

```
Project/
├── streamlit_app.py           ← The web app
├── run_pipeline.py            ← Pipeline executor (called by app)
├── DataSource/               ← Upload folder (app manages this)
├── output/                   ← Results folder (app displays this)
├── tasks/                    ← 6 AI agents
└── requirements.txt          ← Includes streamlit
```

## 🛠️ Technical Details

### How It Works
1. **File Upload**: Saves files to `DataSource/` folder
2. **Pipeline Execution**: Calls `subprocess.Popen(["python", "run_pipeline.py"])`
3. **Log Streaming**: Captures stdout/stderr in real-time
4. **Results Display**: Scans `output/` folder for generated files

### Dependencies
- **Streamlit**: Web interface framework
- **Threading**: Non-blocking pipeline execution
- **Subprocess**: Isolated pipeline execution
- **All existing dependencies**: Pipeline runs unchanged

## 🎨 UI Overview

```
┌─────────────────────────────────────────────────────┐
│  📊 Contact List Consolidation Pipeline             │
├─────────────────────────────────────────────────────┤
│ 📁 File Management           │ 🔍 Pipeline Status    │
│ ┌─────────────────────────┐   │ ┌─────────────────┐   │
│ │ 1. Upload Files         │   │ │ Live Logs       │   │
│ │ [Drag & Drop]           │   │ │ 2025-01-01...   │   │
│ │ [📤 Save to DataSource] │   │ │ Agent 1 start   │   │
│ ├─────────────────────────┤   │ │ Processing...   │   │
│ │ 2. Run Pipeline         │   │ │ Agent 2 done    │   │
│ │ 📂 Ready: 5 files       │   │ └─────────────────┘   │
│ │ [🚀 RUN PIPELINE]       │   │                       │
│ ├─────────────────────────┤   │ 📂 Output Files       │
│ │ 3. Download Results     │   │ ┌─────────────────┐   │
│ │ 📂 12 files available   │   │ │ Agent 4 Results │   │
│ │ [📦 Download All ZIP]   │   │ │ [📥 Download]   │   │
│ └─────────────────────────┘   │ │ Agent 6 CRM     │   │
│                               │ │ [📥 Download]   │   │
│                               │ └─────────────────┘   │
└─────────────────────────────────────────────────────┘
```

## 🔧 Troubleshooting

### Port Already in Use
If you get "Port 8501 is already in use":
```bash
streamlit run streamlit_app.py --server.port 8502
```

### Pipeline Not Found
Ensure you're running from the project root directory:
```bash
cd /path/to/your/project
streamlit run streamlit_app.py
```

### File Upload Issues
- Check `DataSource/` folder permissions
- Ensure sufficient disk space
- Supported formats: CSV, XLSX, XLS

## 🎯 Production Notes

- **Thread-safe**: Multiple users can use simultaneously
- **Resource-efficient**: Only runs pipeline when requested
- **Error-handled**: Graceful failure reporting
- **Scalable**: Can be deployed to cloud platforms

## 📞 Support

The Streamlit app is designed to work **exactly like your existing `run_pipeline.py`** workflow, but with a beautiful web interface. If you encounter any issues, the app provides detailed error logs and status reporting.

**Ready to use!** 🚀
