# Contact Consolidation Pipeline

A Python pipeline that processes messy contact data using 5 AI agents to clean, deduplicate, and enrich email addresses.

## What It Does

Takes messy Excel/CSV files with contact data → Outputs clean, deduplicated contacts with enriched emails.

## The 5 Agents

1. **Agent 1 (File Converter)** - Converts Excel/CSV files to standardized CSV format
2. **Agent 2 (Column Mapper)** - Maps different column names to standard format using AI
3. **Agent 3 (Data Consolidator)** - Combines all files into one dataset
4. **Agent 4 (Smart Deduplicator)** - Removes duplicate contacts using AI logic
5. **Agent 5 (Email Enrichment)** - Finds missing emails by web scraping + AI extraction

## Setup

### Prerequisites
- Python 3.11+
- Ollama with Gemma3:4b model

### Install Ollama & Model
```bash
# Install Ollama
curl -fsSL https://ollama.ai/install.sh | sh

# Download the model
ollama pull gemma3:4b

# Start Ollama (keep running in background)
ollama serve
```

### Install Dependencies
```bash
pip install -r requirements.txt
```

## How to Use

1. **Put your contact files** in the `DataSource/` folder
   - Supports `.xlsx` and `.csv` files
   - Any column names (AI will map them)

2. **Make sure Ollama is running** with Gemma3:4b model

3. **Run the pipeline:**
```bash
python run_pipeline.py
```

4. **Wait for completion** (can take 30-60 minutes depending on file size)

5. **Check results** in the `output/` folder:
   ```
   output/
   ├── agent_1_file_converter/     # Converted CSV files
   ├── agent_2_column_mapper/      # Mapped columns
   ├── agent_3_data_consolidator/  # Combined data
   ├── agent_4_smart_deduplicator/ # Deduplicated contacts
   └── agent_5_email_enrichment/   # Final enriched contacts
   ```

The final enriched contacts are in `agent_5_email_enrichment/`.

## Standard Columns

All output uses these columns:
- First Name, Last Name
- Current Company, Designation / Role
- Email, Phone Number
- LinkedIn Profile URL, Geo (Location by City)

## Troubleshooting

**Ollama not working?**
```bash
ollama list  # Check if model is installed
ollama serve # Make sure it's running
```

**Pipeline fails?** Check the logs in terminal for specific errors.

**Out of memory?** Process smaller batches of files.

That's it! The pipeline handles the rest automatically. 