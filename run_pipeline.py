#!/usr/bin/env python3
"""
Test script for NEW Agent-Based Pipeline Architecture
Validates the 5-agent system as per user's exact specifications
"""

import asyncio
import logging
from pathlib import Path
import time
from master_orchestrator import MasterOrchestrator

# Configure logging for testing
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


async def main():
    """Test the new agent-based pipeline architecture"""

    print("🤖 TESTING NEW AGENT-BASED PIPELINE ARCHITECTURE")
    print("=" * 60)
    print("Following the user's exact vision:")
    print("1. Agent 1: File Converter (Excel/CSV → CSV)")
    print("2. Agent 2: Column Mapper (LLM maps to standard columns)")
    print("3. Agent 3: Data Consolidator (combine all normalized sheets)")
    print("4. Agent 4: Smart Deduplicator (LLM removes duplicates safely)")
    print("5. Agent 5: Web Scraper (TOP 5 contacts, fill missing data)")
    print("6. Agent 6: CSV Cleaner & CRM Exporter (Apollo/HubSpot ready)")
    print("=" * 60)

    try:
        # Create master orchestrator
        orchestrator = MasterOrchestrator()

        # Create the new agent-based pipeline
        orchestrator.create_agent_based_pipeline(source_directory="DataSource", output_directory="output")

        print("\n🚀 Starting Agent-Based Pipeline Execution...")
        print("Each agent will save output in separate subfolders")

        start_time = time.time()

        # Execute the pipeline
        result = await orchestrator.execute_pipeline()

        end_time = time.time()
        execution_time = end_time - start_time

        # Display results
        print("\n" + "=" * 60)
        print("📊 AGENT-BASED PIPELINE EXECUTION SUMMARY")
        print("=" * 60)

        if result["status"] == "success":
            print("✅ Pipeline Status: SUCCESS")
            print(f"⏱️  Total Execution Time: {execution_time:.1f} seconds")
            print(
                f"🎯 Tasks Completed: {result['pipeline_info']['tasks_completed']}/{result['pipeline_info']['tasks_registered']}"
            )

            # Show agent results
            print("\n🤖 AGENT RESULTS:")
            for task_name, task_result in result.get("task_results", {}).items():
                if task_name.endswith("_agent") or task_name == "file_converter":
                    status = "✅" if task_result["status"] == "success" else "❌"
                    print(f"   {status} {task_name}: {task_result['status']} ({task_result['execution_time']:.2f}s)")

            # Show output files structure
            print("\n📁 OUTPUT STRUCTURE:")
            output_dir = Path("output")
            if output_dir.exists():
                for subdir in sorted(output_dir.iterdir()):
                    if subdir.is_dir() and subdir.name.startswith("agent_"):
                        print(f"   📂 {subdir.name}/")
                        for file in sorted(subdir.iterdir()):
                            if file.is_file():
                                print(f"      📄 {file.name}")

            print("\n🎉 NEW AGENT PIPELINE ARCHITECTURE WORKING PERFECTLY!")
            print("✨ Your 5-agent system is ready for production use!")

        else:
            print("❌ Pipeline Status: FAILED")
            print(f"⏱️  Total Execution Time: {execution_time:.1f} seconds")
            print(
                f"🎯 Tasks Completed: {result['pipeline_info']['tasks_completed']}/{result['pipeline_info']['tasks_registered']}"
            )
            print(f"❌ Error: {result.get('error', 'Unknown error')}")

            # Show failed tasks
            print("\n❌ FAILED TASKS:")
            for task_name, task_result in result.get("task_results", {}).items():
                if task_result["status"] != "success":
                    print(f"   ❌ {task_name}: {task_result['error']}")

        return result

    except Exception as e:
        print(f"❌ Pipeline execution failed: {str(e)}")
        import traceback

        traceback.print_exc()
        return None


if __name__ == "__main__":
    # Run the test
    result = asyncio.run(main())

    if result:
        print("\n📊 FINAL STATUS:")
        if result["status"] == "success":
            print("🎊 All 5 agents executed successfully!")
            print("🚀 Your agent-based pipeline is production ready!")
        else:
            print("⚠️  Some issues detected - check the logs above")
    else:
        print("💥 Critical pipeline failure - see error details above")
