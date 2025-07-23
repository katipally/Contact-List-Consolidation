# AI-Powered Contact Consolidation Pipeline

A modern Python 3.11+ application that consolidates and enriches contact data using a 5-agent architecture with LLM integration.

## Features

🤖 **5-Agent Architecture**
- **Agent 1**: File Converter (Excel/CSV → CSV)
- **Agent 2**: Enhanced Column Mapper (LLM maps to standard columns)
- **Agent 3**: Data Consolidator (combine all normalized sheets)
- **Agent 4**: Smart Deduplicator (LLM removes duplicates safely)
- **Agent 5**: Web Scraper (TOP 5 contacts, fill missing data)

🚀 **2025 Technology Stack**
- Python 3.11+ with modern async/await patterns
- Ollama integration for local LLM processing
- RapidFuzz C++ optimization for 8x faster fuzzy matching
- Advanced memory management with performance monitoring
- Circuit breaker pattern for resilience

⚡ **Performance Optimizations**
- Memory-efficient TF-IDF semantic analysis
- Intelligent blocking for large datasets
- Real-time performance monitoring
- Adaptive optimization algorithms

## Quick Start

### Prerequisites

- Python 3.11 or higher
- Ollama with DeepSeek-r1:8b model

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd CL

# Install dependencies (modern way)
pip install -e .

# Or using uv (recommended for 2025)
uv pip install -e .
```

### Running the Pipeline

```bash
# Run the complete 5-agent pipeline
python test_new_agent_pipeline.py
```

### Project Structure

```
CL/
├── DataSource/                 # Input Excel/CSV files
├── output/                     # Agent outputs (separate subfolders)
│   ├── agent_1_file_converter/
│   ├── agent_2_column_mapper/
│   ├── agent_3_data_consolidator/
│   ├── agent_4_smart_deduplicator/
│   └── agent_5_web_scraper/
├── tasks/                      # Agent implementations
├── utils/                      # Utility modules
├── enhanced_pipeline_main.py   # Main entry point
└── master_orchestrator.py     # Pipeline orchestration
```

## Standard Columns

The pipeline normalizes all contact data to these standard columns:

- **First Name**
- **Last Name**
- **Current Company**
- **Designation / Role**
- **LinkedIn Profile URL**
- **Email**
- **Phone Number**
- **Geo (Location by City)**

## Configuration

The pipeline uses modern Python configuration patterns:

- `pyproject.toml` for project metadata and dependencies
- Environment variables for API keys
- LLM models via Ollama local installation

## Development

### Code Quality

This project follows 2025 Python best practices:

```bash
# Format code
ruff format .

# Lint code
ruff check .

# Type checking
mypy .

# Run tests
pytest
```

### Modern Python Features Used

- **Type hints** throughout
- **Dataclasses** for data objects
- **Async/await** for concurrent operations
- **Context managers** for resource management
- **Pathlib** for file operations
- **F-strings** for string formatting

## Performance

The pipeline is optimized for:

- **Speed**: 8x faster fuzzy matching with RapidFuzz C++
- **Memory**: Advanced memory management with `__slots__`
- **Reliability**: Circuit breaker patterns and intelligent fallbacks
- **Safety**: Conservative 85%+ preservation rate for contacts

## Contributing

1. Follow the existing code style (ruff format)
2. Add type hints to all functions
3. Write tests for new features
4. Update documentation

## License

MIT License - see LICENSE file for details. 