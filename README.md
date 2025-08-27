# Contact List Consolidation Tool

## Quick Setup

### What You Need
- Python 3.11 or newer
- About 4GB of free space (for the AI model)
- Internet connection (for finding emails)

### Step 1: Install the AI Brain
```bash
# Install Ollama (the AI engine)
curl -fsSL https://ollama.ai/install.sh | sh

# Download the smart model (this takes a few minutes)
ollama pull gemma3:4b

# Start the AI service (keep this running)
ollama serve
```

### Step 2: Install Python Dependencies
```bash
# Install all the required packages
pip install -r requirements.txt
```

### Step 3: Test Everything Works
```bash
# Quick test - should show the model is ready
ollama list
```

## How to Use This Tool

### Method 1: Easy Web Interface

1. **Start the web app:**
```bash
streamlit run streamlit_app.py
```

2. **Open your browser** - it will automatically open to `http://localhost:8501`

3. **Upload your files** using the drag-and-drop interface

4. **Click "Run Pipeline"**

5. **Download clean contacts** when it's done

### Method 2: Command Line

1. **Put your files** in the `DataSource/` folder
   - Any Excel (.xlsx) or CSV files

2. **Make sure Ollama is running:**
```bash
ollama serve
```

3. **Run the pipeline:**
```bash
python run_pipeline.py
```

4. **Wait for it to finish**

5. **Get your results** from the `output/` folder:
   ```
   output/
   ├── agent_6_csv_cleaner_crm_exporter/  ← Your final clean files are here!
   ├── agent_4_smart_deduplicator/        ← Deduplicated contacts
   ├── agent_5_email_enrichment/          ← With found emails
   └── (other intermediate steps...)
   ```

**💡 Your final, clean contacts are always in the `agent_6_csv_cleaner_crm_exporter/` folder.**

## What You Get

Your clean contact files will have these standard columns:
- **First Name** & **Last Name**
- **Current Company** & **Designation / Role** (job title)
- **Email** & **Phone Number**
- **LinkedIn Profile URL**
- **Geo (Location by City)**

## Troubleshooting

### Common Issues

**🤖 AI not working?**
```bash
# Check if the model is installed
ollama list

# Start the AI service
ollama serve

# If model is missing, download it
ollama pull gemma3:4b
```

**❌ Pipeline crashes?**
- Check the terminal for error messages
- Make sure you have enough free space (4GB+)
- Try processing smaller files first

**🐌 Too slow?**
- This is normal! Processing hundreds of contacts takes time
- The AI is doing smart work, not just simple matching
- Grab a coffee and let it run ☕

**📁 Can't find output files?**
- Look in the `output/agent_6_csv_cleaner_crm_exporter/` folder
- Files are named with timestamps so you don't overwrite old ones

**💾 Out of memory?**
- Process smaller files (under 1000 contacts at a time)
- Close other applications while running